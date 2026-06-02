/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.ObjectType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ObjectFieldStitch}: confirms the rewriter strips
 * {@link ObjectType} columns from a {@link LogicalTableScan}, expands top-level
 * Project references to leaf-column projections, and yields a {@link
 * ObjectFieldStitch.StitchPlan} that re-assembles nested {@code Map<String,Object>}s
 * from engine-output rows.
 */
public class ObjectFieldStitchTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * `| fields city.name` (the existing leaf path): the schema exposes both the flat leaf
     * AND the parent ObjectType column. The user-built plan references only the leaf, so
     * the rewriter strips the ObjectType from the scan but produces a passthrough plan.
     */
    public void testLeafProjectionStripsParentButPassesThrough() {
        RelOptTable table = buildCityTable();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        // Project city.name (idx 1 in the schema below — see buildCityTable).
        RexNode cityNameRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityNameRef), List.of("city.name"), java.util.Set.of());

        ObjectFieldStitch.StitchPlan plan = ObjectFieldStitch.rewrite(project);
        assertFalse("Leaf-only projection needs no stitch", plan.needsStitch());
        assertEquals(List.of("city.name"), plan.outputNames());

        // Rewritten plan: top Project (passthrough of remapped leaf RexInputRef) → stripped
        // TableScan (leaf-only row type, ObjectType parents removed).
        RelNode rewritten = plan.rewrittenPlan();
        assertTrue(rewritten instanceof LogicalProject);
        RelNode rewrittenScan = rewritten.getInput(0);
        assertTrue(rewrittenScan instanceof LogicalTableScan);
        for (var f : rewrittenScan.getRowType().getFieldList()) {
            assertFalse(
                "ObjectType column [" + f.getName() + "] must be stripped from scan",
                f.getType() instanceof ObjectType
            );
        }
    }

    /**
     * `| fields city`: top-level Project selects the parent ObjectType column. The rewriter
     * expands it into leaf projections and emits a Stitch output describing how to
     * reassemble the nested Map.
     */
    public void testParentProjectionExpandsAndStitches() {
        RelOptTable table = buildCityTable();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        // The 'city' parent ObjectType is at index 5 (see buildCityTable).
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityRef), List.of("city"), java.util.Set.of());

        ObjectFieldStitch.StitchPlan plan = ObjectFieldStitch.rewrite(project);
        assertTrue("Parent projection must produce a stitch", plan.needsStitch());
        assertEquals(List.of("city"), plan.outputNames());
        assertTrue(plan.outputs().get(0) instanceof ObjectFieldStitch.OutputColumn.Stitch);

        // Engine row produced by the rewritten project: 3 leaves (name, latitude, longitude)
        // and 1 nested-object intermediate (location → another stitch).
        // Apply stitchRow to a synthetic engine row and confirm the nested Map structure.
        ObjectFieldStitch.OutputColumn.Stitch stitch = (ObjectFieldStitch.OutputColumn.Stitch) plan.outputs().get(0);
        // The Stitch has children [name, population, location]; location is nested.
        assertEquals(java.util.Set.of("name", "population", "location"), stitch.children().keySet());
        assertTrue(stitch.children().get("name") instanceof ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn);
        assertTrue(stitch.children().get("population") instanceof ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn);
        assertTrue(stitch.children().get("location") instanceof ObjectFieldStitch.OutputColumn.ChildSource.NestedStitch);

        // The rewritten Project's row type has exactly the columns it stitches from
        // (4 leaves: name, population, latitude, longitude).
        LogicalProject rewrittenProj = (LogicalProject) plan.rewrittenPlan();
        assertEquals(4, rewrittenProj.getRowType().getFieldCount());

        // Map indices to a simulated engine row and verify the produced Map.
        Object[] engineRow = new Object[4];
        engineRow[((ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn) stitch.children().get("name")).engineColumnIndex()] = "Seattle";
        engineRow[((ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn) stitch.children().get("population")).engineColumnIndex()] = 750000;
        ObjectFieldStitch.OutputColumn.Stitch loc = ((ObjectFieldStitch.OutputColumn.ChildSource.NestedStitch) stitch.children().get("location")).stitch();
        engineRow[((ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn) loc.children().get("latitude")).engineColumnIndex()] = 47.6;
        engineRow[((ObjectFieldStitch.OutputColumn.ChildSource.LeafColumn) loc.children().get("longitude")).engineColumnIndex()] = -122.3;

        Object[] stitched = ObjectFieldStitch.stitchRow(engineRow, plan.outputs());
        assertEquals(1, stitched.length);
        @SuppressWarnings("unchecked")
        Map<String, Object> city = (Map<String, Object>) stitched[0];
        Map<String, Object> expectedLocation = new LinkedHashMap<>();
        expectedLocation.put("latitude", 47.6);
        expectedLocation.put("longitude", -122.3);
        Map<String, Object> expectedCity = new LinkedHashMap<>();
        expectedCity.put("name", "Seattle");
        expectedCity.put("population", 750000);
        expectedCity.put("location", expectedLocation);
        assertEquals(expectedCity, city);
    }

    /**
     * Mixed projection: `| fields city.name, city`. One leaf passthrough + one stitched
     * parent. The engine row carries (city.name leaf, plus stitch leaves), the stitcher
     * produces (city.name string, city Map).
     */
    /**
     * `| fields city | head 3` — the topmost Project sits below a LogicalSort. The rewriter
     * must walk through the Sort, strip the scan, and identify the Project as the topmost
     * one to expand into a Stitch. Indices in upstream operators (e.g. Sort fetch) need to
     * remain valid after the strip.
     */
    public void testParentProjectionUnderSort() {
        RelOptTable table = buildCityTable();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityRef), List.of("city"), java.util.Set.of());
        LogicalSort sort = LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        ObjectFieldStitch.StitchPlan plan = ObjectFieldStitch.rewrite(sort);
        assertTrue(plan.needsStitch());
        assertEquals(List.of("city"), plan.outputNames());

        // The rewritten root must still be a Sort; below it sits the rewritten LogicalProject
        // that projects leaves; below that, the stripped scan.
        assertTrue(plan.rewrittenPlan() instanceof LogicalSort);
        RelNode innerProject = plan.rewrittenPlan().getInput(0);
        assertTrue(innerProject instanceof LogicalProject);
        // 4 leaves: name, population, latitude, longitude.
        assertEquals(4, innerProject.getRowType().getFieldCount());
    }

    public void testMixedLeafAndParentProjection() {
        RelOptTable table = buildCityTable();
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        int cityNameIdx = scan.getRowType().getField("city.name", true, false).getIndex();
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode nameRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityNameIdx).getType(), cityNameIdx);
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(nameRef, cityRef), List.of("city.name", "city"), java.util.Set.of());

        ObjectFieldStitch.StitchPlan plan = ObjectFieldStitch.rewrite(project);
        assertTrue(plan.needsStitch());
        assertEquals(List.of("city.name", "city"), plan.outputNames());
        assertTrue(plan.outputs().get(0) instanceof ObjectFieldStitch.OutputColumn.Passthrough);
        assertTrue(plan.outputs().get(1) instanceof ObjectFieldStitch.OutputColumn.Stitch);
    }

    /**
     * Build a Calcite RelOptTable mirroring the city_index mapping the schema test exercises,
     * with both flat-leaf and ObjectType-parent columns laid out in deterministic order.
     */
    private RelOptTable buildCityTable() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR));                      // 0
        b.add("city.name", typeFactory.createSqlType(SqlTypeName.VARCHAR));               // 1
        b.add("city.population", typeFactory.createSqlType(SqlTypeName.INTEGER));         // 2
        b.add("city.location.latitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));   // 3
        b.add("city.location.longitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));  // 4

        // city.location parent (nested ObjectType)
        Map<String, ObjectType.Child> locChildren = new LinkedHashMap<>();
        locChildren.put("latitude", new ObjectType.Child.Leaf("city.location.latitude"));
        locChildren.put("longitude", new ObjectType.Child.Leaf("city.location.longitude"));
        ObjectType locType = new ObjectType(true, locChildren);

        // city parent (top-level ObjectType)
        Map<String, ObjectType.Child> cityChildren = new LinkedHashMap<>();
        cityChildren.put("name", new ObjectType.Child.Leaf("city.name"));
        cityChildren.put("population", new ObjectType.Child.Leaf("city.population"));
        cityChildren.put("location", new ObjectType.Child.Nested(locType));
        ObjectType cityType = new ObjectType(true, cityChildren);

        b.add("city", cityType);                                                          // 5
        b.add("city.location", locType);                                                  // 6
        RelDataType rowType = b.build();

        return new RelOptAbstractTable(null, "city_index", rowType) {};
    }
}
