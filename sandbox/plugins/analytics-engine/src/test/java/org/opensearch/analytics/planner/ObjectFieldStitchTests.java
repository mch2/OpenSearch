/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.ObjectType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for {@link ObjectFieldStitch}. Coverage matrix:
 * <ul>
 *   <li><b>Strip + remap</b>: leaf-only projection, no-ObjectType short-circuit</li>
 *   <li><b>Top-Project expansion</b>: bare parent, mixed leaf+parent, parent-under-Sort,
 *       intermediate-level parent, multi-parent siblings</li>
 *   <li><b>Filter expansion (positive)</b>: {@code IS_NOT_NULL(parent)}, {@code IS_NULL(parent)},
 *       NOT-wrapped, AND-wrapped with a leaf predicate, IS_NOT_NULL on a nested parent</li>
 *   <li><b>Filter rejection</b>: equality, IS_NOT_NULL on a computed expression</li>
 *   <li><b>Window expansion</b>: PARTITION BY parent, mixed PARTITION BY parent+leaf,
 *       multi-parent partition, nested-object partition</li>
 *   <li><b>Sort rejection</b>: sort by parent; sort by leaf below parent-projecting top</li>
 *   <li><b>Multi-input rejection</b>: Join (covered by integration tests too)</li>
 * </ul>
 */
public class ObjectFieldStitchTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RexBuilder rex;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rex = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rex);
    }

    // ── Strip + remap ─────────────────────────────────────────────────────────────

    /**
     * `| fields city.name`: schema exposes both flat leaf AND parent ObjectType column;
     * the user only references the leaf. Rewriter strips ObjectType columns from the scan
     * and produces a passthrough plan + a passthrough stitch.
     */
    public void testLeafProjectionStripsParentButPassesThrough() {
        LogicalTableScan scan = scanCityTable();
        LogicalProject project = projectByName(scan, List.of("city.name"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city.name"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof Stitch.Output.Passthrough);

        RelNode rewrittenScan = rewrite.plan().getInput(0);
        assertTrue(rewrittenScan instanceof LogicalTableScan);
        for (var f : rewrittenScan.getRowType().getFieldList()) {
            assertFalse("ObjectType column [" + f.getName() + "] must be stripped from scan", f.getType() instanceof ObjectType);
        }
    }

    /** No ObjectType columns anywhere in the plan: rewriter is a no-op via short-circuit. */
    public void testNoObjectTypeColumnsReturnsEmpty() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RelOptTable flatTable = new RelOptAbstractTable(null, "flat_index", b.build()) {};
        LogicalTableScan scan = LogicalTableScan.create(cluster, flatTable, List.of());
        LogicalProject project = projectByName(scan, List.of("name"));

        assertTrue("Plan with no ObjectType columns should short-circuit", ObjectFieldStitch.maybeRewrite(project).isEmpty());
    }

    // ── Top-Project expansion ─────────────────────────────────────────────────────

    /**
     * `| fields city`: top-level Project selects the parent ObjectType. Rewriter expands
     * it into leaf projections and emits an ObjectMap output that re-assembles the leaves
     * into a nested {@code Map<String,Object>} at row time.
     */
    public void testParentProjectionExpandsAndStitches() {
        LogicalTableScan scan = scanCityTable();
        LogicalProject project = projectByName(scan, List.of("city"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof Stitch.Output.ObjectMap);

        Stitch.Output.ObjectMap stitch = (Stitch.Output.ObjectMap) rewrite.stitch().outputs().get(0);
        assertEquals(Set.of("name", "population", "location"), stitch.children().keySet());
        assertTrue(stitch.children().get("name") instanceof Stitch.MapSource.Leaf);
        assertTrue(stitch.children().get("population") instanceof Stitch.MapSource.Leaf);
        assertTrue(stitch.children().get("location") instanceof Stitch.MapSource.Nested);

        // 4 leaves emitted by the rewritten Project (name, population, latitude, longitude).
        assertEquals(4, ((LogicalProject) rewrite.plan()).getRowType().getFieldCount());

        // Round-trip a synthetic engine row through the stitch and verify the produced map.
        Object[] engineRow = new Object[4];
        engineRow[((Stitch.MapSource.Leaf) stitch.children().get("name")).engineColumnIndex()] = "Seattle";
        engineRow[((Stitch.MapSource.Leaf) stitch.children().get("population")).engineColumnIndex()] = 750000;
        Stitch.MapSource.Nested loc = (Stitch.MapSource.Nested) stitch.children().get("location");
        engineRow[((Stitch.MapSource.Leaf) loc.children().get("latitude")).engineColumnIndex()] = 47.6;
        engineRow[((Stitch.MapSource.Leaf) loc.children().get("longitude")).engineColumnIndex()] = -122.3;

        List<Object[]> stitched = rewrite.stitch().apply(Collections.singletonList(engineRow));
        assertEquals(1, stitched.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> city = (Map<String, Object>) stitched.get(0)[0];
        Map<String, Object> expectedLocation = orderedMap("latitude", 47.6, "longitude", -122.3);
        Map<String, Object> expectedCity = orderedMap("name", "Seattle", "population", 750000, "location", expectedLocation);
        assertEquals(expectedCity, city);
    }

    /** `| fields city | head 3` — top Project sits below a LogicalSort. Walker descends, rewrites. */
    public void testParentProjectionUnderSort() {
        LogicalTableScan scan = scanCityTable();
        LogicalProject project = projectByName(scan, List.of("city"));
        LogicalSort sort = LogicalSort.create(project, RelCollations.EMPTY, null, intLiteral(3));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(sort).orElseThrow();
        assertEquals(List.of("city"), rewrite.stitch().names());
        assertTrue(rewrite.plan() instanceof LogicalSort);
        RelNode innerProject = rewrite.plan().getInput(0);
        assertTrue(innerProject instanceof LogicalProject);
        assertEquals(4, innerProject.getRowType().getFieldCount());
    }

    /** `| fields city.name, city`: one passthrough leaf + one stitched parent. */
    public void testMixedLeafAndParentProjection() {
        LogicalTableScan scan = scanCityTable();
        LogicalProject project = projectByName(scan, List.of("city.name", "city"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city.name", "city"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof Stitch.Output.Passthrough);
        assertTrue(rewrite.stitch().outputs().get(1) instanceof Stitch.Output.ObjectMap);
    }

    /**
     * Two unrelated parent objects in the same projection. Each must produce its own
     * ObjectMap output; their leaves coexist on the engine plan.
     */
    public void testMultipleParentSiblings() {
        LogicalTableScan scan = scanCityAccountTable();
        LogicalProject project = projectByName(scan, List.of("city", "account"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        assertEquals(List.of("city", "account"), rewrite.stitch().names());
        assertTrue(rewrite.stitch().outputs().get(0) instanceof Stitch.Output.ObjectMap);
        assertTrue(rewrite.stitch().outputs().get(1) instanceof Stitch.Output.ObjectMap);
        // city has 4 leaves, account has 2 → engine plan has 6 leaf projections.
        assertEquals(6, ((LogicalProject) rewrite.plan()).getRowType().getFieldCount());
    }

    /** `| fields city.location`: project an intermediate (3rd-level) ObjectType, not the topmost. */
    public void testProjectIntermediateNestedParent() {
        LogicalTableScan scan = scanCityTable();
        LogicalProject project = projectByName(scan, List.of("city.location"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        Stitch.Output.ObjectMap stitch = (Stitch.Output.ObjectMap) rewrite.stitch().outputs().get(0);
        assertEquals(Set.of("latitude", "longitude"), stitch.children().keySet());
        assertEquals("Both children are leaves at this level", 2, stitch.children().size());
    }

    // ── Filter expansion (positive) ───────────────────────────────────────────────

    /**
     * `| where isnotnull(city)` — IS_NOT_NULL on parent expands to OR of IS_NOT_NULL across
     * each transitive leaf. Matches OpenSearch's _exists_ semantic.
     */
    public void testIsNotNullOnObjectParentExpandsToLeafDisjunction() {
        LogicalProject project = filterThenProject(SqlStdOperatorTable.IS_NOT_NULL, "city");

        RexCall expanded = expandedFilterCondition(project);
        assertEquals(SqlStdOperatorTable.OR, expanded.getOperator());
        assertEquals("city has 4 transitive leaves", 4, expanded.getOperands().size());
        for (RexNode operand : expanded.getOperands()) {
            assertEquals(SqlStdOperatorTable.IS_NOT_NULL, ((RexCall) operand).getOperator());
        }
    }

    /** `| where isnull(city)` — symmetric: AND of IS_NULL across leaves. */
    public void testIsNullOnObjectParentExpandsToLeafConjunction() {
        LogicalProject project = filterThenProject(SqlStdOperatorTable.IS_NULL, "city");

        RexCall expanded = expandedFilterCondition(project);
        assertEquals(SqlStdOperatorTable.AND, expanded.getOperator());
        assertEquals(4, expanded.getOperands().size());
    }

    /**
     * `| where not(isnotnull(city))` — IS_NOT_NULL nested under NOT. Shuttle visits the
     * IS_NOT_NULL through visitCall → expansion path inside super.visitCall(NOT)'s recursion.
     */
    public void testNotIsNotNullOnObjectParentExpands() {
        LogicalTableScan scan = scanCityTable();
        RexNode cityRef = inputRef(scan, "city");
        RexNode notNull = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, cityRef);
        RexNode notWrapped = rex.makeCall(SqlStdOperatorTable.NOT, notNull);
        LogicalFilter filter = LogicalFilter.create(scan, notWrapped);
        LogicalProject project = projectByName(filter, List.of("id"));

        RexCall outer = expandedFilterCondition(project);
        assertEquals(SqlStdOperatorTable.NOT, outer.getOperator());
        RexCall inner = (RexCall) outer.getOperands().get(0);
        assertEquals("Inner OR of per-leaf IS_NOT_NULL after expansion", SqlStdOperatorTable.OR, inner.getOperator());
        assertEquals(4, inner.getOperands().size());
    }

    /**
     * `| where isnotnull(city) and id is not null` — parent isnotnull AND a leaf predicate.
     * Confirms the shuttle expands the parent leg without breaking the leaf-side AND child.
     */
    public void testIsNotNullParentAndLeafPredicate() {
        LogicalTableScan scan = scanCityTable();
        RexNode cityNotNull = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, inputRef(scan, "city"));
        RexNode idNotNull = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, inputRef(scan, "id"));
        RexNode and = rex.makeCall(SqlStdOperatorTable.AND, cityNotNull, idNotNull);
        LogicalFilter filter = LogicalFilter.create(scan, and);
        LogicalProject project = projectByName(filter, List.of("id"));

        RexCall outerAnd = expandedFilterCondition(project);
        assertEquals(SqlStdOperatorTable.AND, outerAnd.getOperator());
        assertEquals(2, outerAnd.getOperands().size());
        // Left operand is the OR-of-IS_NOT_NULL expansion (city's leaves).
        RexCall expandedLeg = (RexCall) outerAnd.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.OR, expandedLeg.getOperator());
        assertEquals(4, expandedLeg.getOperands().size());
        // Right operand is the leaf-side IS_NOT_NULL, untouched (apart from index remap).
        RexCall leafLeg = (RexCall) outerAnd.getOperands().get(1);
        assertEquals(SqlStdOperatorTable.IS_NOT_NULL, leafLeg.getOperator());
    }

    /** `| where isnotnull(city.location)` — IS_NOT_NULL on a 2-level-deep nested object. */
    public void testIsNotNullOnNestedObjectExpands() {
        LogicalTableScan scan = scanCityTable();
        RexNode locRef = inputRef(scan, "city.location");
        RexNode notNull = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, locRef);
        LogicalFilter filter = LogicalFilter.create(scan, notNull);
        LogicalProject project = projectByName(filter, List.of("id"));

        RexCall expanded = expandedFilterCondition(project);
        assertEquals(SqlStdOperatorTable.OR, expanded.getOperator());
        assertEquals("city.location has 2 leaves", 2, expanded.getOperands().size());
    }

    // ── Filter rejection ──────────────────────────────────────────────────────────

    /**
     * `| where city = 'Seattle'` — equality on a parent has no defined semantic. The
     * Filter constructor's RexChecker may trip first with an AssertionError; otherwise
     * our shuttle throws IllegalStateException. Either flavor is a valid loud fail.
     */
    public void testEqualityOnObjectParentRejected() {
        LogicalTableScan scan = scanCityTable();
        RexNode cityRef = inputRef(scan, "city");
        RexNode eq = rex.makeCall(SqlStdOperatorTable.EQUALS, cityRef, rex.makeLiteral("Seattle"));
        LogicalFilter filter = LogicalFilter.create(scan, eq);
        LogicalProject project = projectByName(filter, List.of("id"));

        expectThrows(Throwable.class, () -> ObjectFieldStitch.maybeRewrite(project));
    }

    /**
     * `where coalesce(city, city) is not null` — IS_NOT_NULL applied to a >1-arg expression
     * (not a bare InputRef). The shuttle whitelist requires the IS_NOT_NULL operand to be
     * a direct InputRef on a dropped column — anything else falls through to default visit
     * which throws on the embedded RexInputRef.
     */
    public void testIsNotNullOnComputedExprOverParentRejected() {
        LogicalTableScan scan = scanCityTable();
        RexNode cityRef = inputRef(scan, "city");
        RexNode coalesce = rex.makeCall(SqlStdOperatorTable.COALESCE, cityRef, cityRef);
        RexNode notNull = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, coalesce);
        LogicalFilter filter = LogicalFilter.create(scan, notNull);
        LogicalProject project = projectByName(filter, List.of("id"));

        expectThrows(Throwable.class, () -> ObjectFieldStitch.maybeRewrite(project));
    }

    // ── Window expansion ──────────────────────────────────────────────────────────

    /**
     * `| dedup city` — SQL plugin lowers to ROW_NUMBER OVER (PARTITION BY city). Shuttle
     * expands the partition-by InputRef on the parent into the full leaf list.
     */
    public void testDedupOnObjectParentExpandsPartitionByLeaves() {
        LogicalTableScan scan = scanCityTable();
        RexOver rowNumber = rowNumberOver(List.of(inputRef(scan, "city")));
        LogicalProject inner = LogicalProject.create(scan, List.of(), List.of(inputRef(scan, "id"), rowNumber), List.of("id", "_rn"), Set.of());
        LogicalProject top = projectByName(inner, List.of("id"));

        RexOver expanded = findRexOver((LogicalProject) ObjectFieldStitch.maybeRewrite(top).orElseThrow().plan().getInput(0));
        assertEquals("city has 4 transitive leaves", 4, expanded.getWindow().partitionKeys.size());
        for (RexNode key : expanded.getWindow().partitionKeys) {
            assertTrue("Partition key must be a RexInputRef into a leaf column", key instanceof RexInputRef);
        }
    }

    /**
     * `| dedup city, id` — PARTITION BY mixes a parent and a leaf. Parent expands; leaf
     * passes through with its index remapped.
     */
    public void testDedupMixedPartitionByParentAndLeaf() {
        LogicalTableScan scan = scanCityTable();
        RexOver rowNumber = rowNumberOver(List.of(inputRef(scan, "city"), inputRef(scan, "id")));
        LogicalProject inner = LogicalProject.create(scan, List.of(), List.of(inputRef(scan, "id"), rowNumber), List.of("id", "_rn"), Set.of());
        LogicalProject top = projectByName(inner, List.of("id"));

        RexOver expanded = findRexOver((LogicalProject) ObjectFieldStitch.maybeRewrite(top).orElseThrow().plan().getInput(0));
        // 4 leaves from city + 1 leaf from id = 5 partition keys total.
        assertEquals(5, expanded.getWindow().partitionKeys.size());
    }

    /**
     * Two parent partitions in one window. Each expands to its leaf list independently;
     * the keys come out in declaration order.
     */
    public void testDedupTwoParentsInPartitionBy() {
        LogicalTableScan scan = scanCityAccountTable();
        RexOver rowNumber = rowNumberOver(List.of(inputRef(scan, "city"), inputRef(scan, "account")));
        LogicalProject inner = LogicalProject.create(scan, List.of(), List.of(inputRef(scan, "id"), rowNumber), List.of("id", "_rn"), Set.of());
        LogicalProject top = projectByName(inner, List.of("id"));

        RexOver expanded = findRexOver((LogicalProject) ObjectFieldStitch.maybeRewrite(top).orElseThrow().plan().getInput(0));
        // city has 4 leaves + account has 2 leaves = 6 partition keys.
        assertEquals(6, expanded.getWindow().partitionKeys.size());
    }

    /**
     * `| dedup city.location` — PARTITION BY a 2-level-deep nested object. Expands to its
     * 2 leaf children only (latitude, longitude), not all 4 city leaves.
     */
    public void testDedupOnNestedObjectExpandsToNestedLeaves() {
        LogicalTableScan scan = scanCityTable();
        RexOver rowNumber = rowNumberOver(List.of(inputRef(scan, "city.location")));
        LogicalProject inner = LogicalProject.create(scan, List.of(), List.of(inputRef(scan, "id"), rowNumber), List.of("id", "_rn"), Set.of());
        LogicalProject top = projectByName(inner, List.of("id"));

        RexOver expanded = findRexOver((LogicalProject) ObjectFieldStitch.maybeRewrite(top).orElseThrow().plan().getInput(0));
        assertEquals("city.location has 2 leaves", 2, expanded.getWindow().partitionKeys.size());
    }

    // ── Sort rejection ────────────────────────────────────────────────────────────

    /**
     * `| sort city` — Sort over the parent column. The sort collation references the
     * stripped index → loud-fail in rewriteSort.
     */
    public void testSortOnObjectParentRejected() {
        LogicalTableScan scan = scanCityTable();
        int cityIdx = scan.getRowType().getField("city", true, false).getIndex();
        LogicalSort sort = LogicalSort.create(scan, RelCollations.of(new RelFieldCollation(cityIdx)), null, null);
        LogicalProject project = projectByName(sort, List.of("id"));

        expectThrows(IllegalStateException.class, () -> ObjectFieldStitch.maybeRewrite(project));
    }

    /**
     * Sort by a leaf with a parent-projecting top. Confirms the Sort's collation is remapped
     * to the leaf's new index without rejecting (the leaf survives the strip).
     */
    public void testSortOnLeafWithParentProjectionAbove() {
        LogicalTableScan scan = scanCityTable();
        int idIdx = scan.getRowType().getField("id", true, false).getIndex();
        LogicalSort sort = LogicalSort.create(scan, RelCollations.of(new RelFieldCollation(idIdx)), null, null);
        LogicalProject project = projectByName(sort, List.of("city"));

        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        // Plan shape: Project(stitched city) / Sort(by id) / strippedScan.
        RelNode scanLevel = rewrite.plan().getInput(0).getInput(0);
        // After strip, id is at the new index 0 (no ObjectType columns remaining).
        assertEquals(0, scanLevel.getRowType().getField("id", true, false).getIndex());
    }

    // ── Multi-input rejection ─────────────────────────────────────────────────────

    /** Build a synthetic Join (two scans of the city table) and confirm the walker rejects. */
    public void testJoinRejected() {
        LogicalTableScan left = scanCityTable();
        LogicalTableScan right = scanCityTable();
        // Calcite's LogicalJoin needs a condition; use TRUE.
        org.apache.calcite.rel.logical.LogicalJoin join = org.apache.calcite.rel.logical.LogicalJoin.create(
            left,
            right,
            List.of(),
            rex.makeLiteral(true),
            Set.of(),
            org.apache.calcite.rel.core.JoinRelType.INNER
        );
        // The walker rejects on visit(); the wrapping Project just gives us a single root.
        // Project the LHS city only — but Join's row type includes both sides, so a passthrough on idx 0 is fine.
        RexNode firstFieldRef = rex.makeInputRef(join.getRowType().getFieldList().get(0).getType(), 0);
        LogicalProject project = LogicalProject.create(join, List.of(), List.of(firstFieldRef), List.of("x"), Set.of());

        expectThrows(IllegalStateException.class, () -> ObjectFieldStitch.maybeRewrite(project));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────

    /** Build a 2-arg + Project filter pattern: Filter(op(parentRef)) → Project(id). */
    private LogicalProject filterThenProject(org.apache.calcite.sql.SqlOperator op, String parentName) {
        LogicalTableScan scan = scanCityTable();
        RexNode call = rex.makeCall(op, inputRef(scan, parentName));
        LogicalFilter filter = LogicalFilter.create(scan, call);
        return projectByName(filter, List.of("id"));
    }

    /** Walk into the rewritten plan's Filter and return its expanded condition as a RexCall. */
    private RexCall expandedFilterCondition(LogicalProject project) {
        ObjectFieldStitch.Rewrite rewrite = ObjectFieldStitch.maybeRewrite(project).orElseThrow();
        RelNode rewrittenFilter = rewrite.plan().getInput(0);
        assertTrue("Plan inner must be a Filter", rewrittenFilter instanceof LogicalFilter);
        return (RexCall) ((LogicalFilter) rewrittenFilter).getCondition();
    }

    /** Build {@code ROW_NUMBER() OVER (PARTITION BY <keys> ROWS UNBOUNDED PRECEDING TO CURRENT ROW)}. */
    private RexOver rowNumberOver(List<RexNode> partitionKeys) {
        return (RexOver) rex.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            partitionKeys,
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false,
            false
        );
    }

    /** Find the first RexOver in a Project's projection list. */
    private RexOver findRexOver(LogicalProject project) {
        for (RexNode e : project.getProjects()) {
            if (e instanceof RexOver o) return o;
        }
        throw new AssertionError("Inner project must carry the RexOver");
    }

    /** Build a passthrough RexInputRef to a column by name from the input's row type. */
    private RexNode inputRef(RelNode input, String fieldName) {
        int idx = input.getRowType().getField(fieldName, true, false).getIndex();
        return rex.makeInputRef(input.getRowType().getFieldList().get(idx).getType(), idx);
    }

    /** Build a Project that selects {@code fieldNames} as passthrough RexInputRefs. */
    private LogicalProject projectByName(RelNode input, List<String> fieldNames) {
        List<RexNode> exprs = new java.util.ArrayList<>(fieldNames.size());
        for (String name : fieldNames) exprs.add(inputRef(input, name));
        return LogicalProject.create(input, List.of(), exprs, fieldNames, Set.of());
    }

    private RexNode intLiteral(int v) {
        return rex.makeLiteral(v, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    }

    /** Build a small ordered map literal for assertions. */
    private static Map<String, Object> orderedMap(Object... kvs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) m.put((String) kvs[i], kvs[i + 1]);
        return m;
    }

    // ── Synthetic tables ──────────────────────────────────────────────────────────

    /** {@code id, city.name, city.population, city.location.latitude, city.location.longitude, city, city.location}. */
    private LogicalTableScan scanCityTable() {
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
        RelOptTable t = new RelOptAbstractTable(null, "city_index", b.build()) {};
        return LogicalTableScan.create(cluster, t, List.of());
    }

    /**
     * city table extended with an {@code account} parent (owner: VARCHAR, balance: DOUBLE).
     * Used for multi-parent tests. Total ObjectType columns: city, city.location, account.
     */
    private LogicalTableScan scanCityAccountTable() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("city.name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("city.population", typeFactory.createSqlType(SqlTypeName.INTEGER));
        b.add("city.location.latitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        b.add("city.location.longitude", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        b.add("account.owner", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("account.balance", typeFactory.createSqlType(SqlTypeName.DOUBLE));

        Map<String, ObjectType.Child> locChildren = new LinkedHashMap<>();
        locChildren.put("latitude", new ObjectType.Child.Leaf("city.location.latitude"));
        locChildren.put("longitude", new ObjectType.Child.Leaf("city.location.longitude"));
        ObjectType locType = new ObjectType(true, locChildren);

        Map<String, ObjectType.Child> cityChildren = new LinkedHashMap<>();
        cityChildren.put("name", new ObjectType.Child.Leaf("city.name"));
        cityChildren.put("population", new ObjectType.Child.Leaf("city.population"));
        cityChildren.put("location", new ObjectType.Child.Nested(locType));
        ObjectType cityType = new ObjectType(true, cityChildren);

        Map<String, ObjectType.Child> accountChildren = new LinkedHashMap<>();
        accountChildren.put("owner", new ObjectType.Child.Leaf("account.owner"));
        accountChildren.put("balance", new ObjectType.Child.Leaf("account.balance"));
        ObjectType accountType = new ObjectType(true, accountChildren);

        b.add("city", cityType);
        b.add("city.location", locType);
        b.add("account", accountType);
        RelOptTable t = new RelOptAbstractTable(null, "city_account_index", b.build()) {};
        return LogicalTableScan.create(cluster, t, List.of());
    }
}
