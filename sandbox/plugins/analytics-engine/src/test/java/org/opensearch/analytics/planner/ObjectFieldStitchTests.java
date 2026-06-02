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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.ObjectType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link ObjectFieldStitch}: confirms scan-strip, leaf-passthrough remap,
 * top-level ObjectType expansion, and end-to-end row stitching against a synthetic engine row.
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
     * `| fields city.name`: schema exposes both the flat leaf AND the parent ObjectType
     * column, but the user only references the leaf. The rewriter strips ObjectType columns
     * from the scan and produces a passthrough plan + a passthrough stitch.
     */
    public void testLeafProjectionStripsParentButPassesThrough() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, buildCityTable(), List.of());
        RexNode cityNameRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityNameRef), List.of("city.name"), java.util.Set.of());

        Optional<ObjectFieldStitch.Rewrite> rewrite = ObjectFieldStitch.maybeRewrite(project);
        assertTrue(rewrite.isPresent());
        assertEquals(List.of("city.name"), rewrite.get().stitch().names());
        assertTrue(rewrite.get().stitch().outputs().get(0) instanceof ObjectFieldStitch.Output.Passthrough);

        // Top Project (passthrough RexInputRef) → stripped TableScan (leaves only).
        RelNode rewritten = rewrite.get().plan();
        assertTrue(rewritten instanceof LogicalProject);
        RelNode rewrittenScan = rewritten.getInput(0);
        assertTrue(rewrittenScan instanceof LogicalTableScan);
        for (var f : rewrittenScan.getRowType().getFieldList()) {
            assertFalse("ObjectType column [" + f.getName() + "] must be stripped from scan", f.getType() instanceof ObjectType);
        }
    }

    /**
     * `| fields city`: top-level Project selects the parent ObjectType. The rewriter expands
     * it into leaf projections and emits an ObjectMap output that re-assembles the leaves
     * into a nested {@code Map<String,Object>}.
     */
    public void testParentProjectionExpandsAndStitches() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, buildCityTable(), List.of());
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityRef), List.of("city"), java.util.Set.of());

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof ObjectFieldStitch.Output.ObjectMap);

        ObjectFieldStitch.Output.ObjectMap stitch = (ObjectFieldStitch.Output.ObjectMap) rewrite.stitch().outputs().get(0);
        assertEquals(java.util.Set.of("name", "population", "location"), stitch.children().keySet());
        assertTrue(stitch.children().get("name") instanceof ObjectFieldStitch.MapSource.Leaf);
        assertTrue(stitch.children().get("population") instanceof ObjectFieldStitch.MapSource.Leaf);
        assertTrue(stitch.children().get("location") instanceof ObjectFieldStitch.MapSource.Nested);

        // 4 leaves emitted by the rewritten Project (name, population, latitude, longitude).
        assertEquals(4, ((LogicalProject) rewrite.plan()).getRowType().getFieldCount());

        // Apply the stitch to a synthetic engine row and verify the produced nested Map.
        Object[] engineRow = new Object[4];
        engineRow[((ObjectFieldStitch.MapSource.Leaf) stitch.children().get("name")).engineColumnIndex()] = "Seattle";
        engineRow[((ObjectFieldStitch.MapSource.Leaf) stitch.children().get("population")).engineColumnIndex()] = 750000;
        ObjectFieldStitch.MapSource.Nested loc = (ObjectFieldStitch.MapSource.Nested) stitch.children().get("location");
        engineRow[((ObjectFieldStitch.MapSource.Leaf) loc.children().get("latitude")).engineColumnIndex()] = 47.6;
        engineRow[((ObjectFieldStitch.MapSource.Leaf) loc.children().get("longitude")).engineColumnIndex()] = -122.3;

        List<Object[]> stitched = rewrite.stitch().apply(java.util.Collections.singletonList(engineRow));
        assertEquals(1, stitched.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> city = (Map<String, Object>) stitched.get(0)[0];
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
     * `| fields city | head 3` — top Project sits below a LogicalSort. The walker descends
     * through the Sort, identifies the Project, and rewrites it to leaf projections.
     */
    public void testParentProjectionUnderSort() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, buildCityTable(), List.of());
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(cityRef), List.of("city"), java.util.Set.of());
        LogicalSort sort = LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(sort).orElseThrow();
        assertEquals(List.of("city"), rewrite.stitch().names());
        // Plan shape: Sort → Project (leaves) → stripped Scan.
        assertTrue(rewrite.plan() instanceof LogicalSort);
        RelNode innerProject = rewrite.plan().getInput(0);
        assertTrue(innerProject instanceof LogicalProject);
        assertEquals(4, innerProject.getRowType().getFieldCount());
    }

    /** `| fields city.name, city`: one passthrough leaf + one stitched parent. */
    public void testMixedLeafAndParentProjection() {
        LogicalTableScan scan = LogicalTableScan.create(cluster, buildCityTable(), List.of());
        int cityNameIdx = scan.getRowType().getField("city.name", true, false).getIndex();
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        RexNode nameRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityNameIdx).getType(), cityNameIdx);
        RexNode cityRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(cityIdx).getType(), cityIdx);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(nameRef, cityRef), List.of("city.name", "city"), java.util.Set.of());

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city.name", "city"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof ObjectFieldStitch.Output.Passthrough);
        assertTrue(rewrite.stitch().outputs().get(1) instanceof ObjectFieldStitch.Output.ObjectMap);
    }

    /** No ObjectType columns anywhere in the plan: rewriter is a no-op via short-circuit. */
    public void testNoObjectTypeColumnsReturnsEmpty() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RelOptTable flatTable = new RelOptAbstractTable(null, "flat_index", b.build()) {};
        LogicalTableScan scan = LogicalTableScan.create(cluster, flatTable, List.of());
        RexNode nameRef = rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1);
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(nameRef), List.of("name"), java.util.Set.of());

        assertTrue("Plan with no ObjectType columns should short-circuit", ObjectFieldStitch.maybeRewrite(project).isEmpty());
    }

    /** Build a Calcite table mirroring the city_index mapping with both flat leaves and ObjectType parents. */
    private RelOptTable buildCityTable() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("city.name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("city.population", typeFactory.createSqlType(SqlTypeName.INTEGER));
        b.add("city.location.latitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        b.add("city.location.longitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));

        Map<String, ObjectType.Child> locChildren = new LinkedHashMap<>();
        locChildren.put("latitude", new ObjectType.Child.Leaf("city.location.latitude"));
        locChildren.put("longitude", new ObjectType.Child.Leaf("city.location.longitude"));
        ObjectType locType = new ObjectType(true, locChildren);

        Map<String, ObjectType.Child> cityChildren = new LinkedHashMap<>();
        cityChildren.put("name", new ObjectType.Child.Leaf("city.name"));
        cityChildren.put("population", new ObjectType.Child.Leaf("city.population"));
        cityChildren.put("location", new ObjectType.Child.Nested(locType));
        ObjectType cityType = new ObjectType(true, cityChildren);

        b.add("city", cityType);
        b.add("city.location", locType);
        return new RelOptAbstractTable(null, "city_index", b.build()) {};
    }
}
