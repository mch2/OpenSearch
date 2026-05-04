/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SortRel;

/**
 * Tests for {@link DataFusionFragmentConvertor}. Each conversion method is
 * exercised independently against a Calcite RelNode constructed in-process,
 * the returned Substrait proto bytes are decoded back into proto structures,
 * and assertions are made on proto shape — not serialized string content.
 *
 */
public class DataFusionFragmentConvertorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        // Load the Substrait extension catalog with the test classloader as TCCL —
        // mirrors the swap performed by DataFusionPlugin#loadSubstraitExtensions.
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DataFusionFragmentConvertorTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            // Mirror DataFusionPlugin#loadSubstraitExtensions: layer the plugin-local
            // opensearch_*.yaml catalogs on top of substrait-core's defaults so tests
            // resolve PPL-specific aliases and function signatures (e.g. scalar_min →
            // least, array → make_array, take UDAF).
            collection = mergeClasspathYaml(collection, "/extensions/opensearch_aggregate.yaml");
            collection = mergeClasspathYaml(collection, "/extensions/opensearch_scalar.yaml");
            extensions = collection;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private DataFusionFragmentConvertor newConvertor() {
        return new DataFusionFragmentConvertor(extensions);
    }

    private static SimpleExtension.ExtensionCollection mergeClasspathYaml(
        SimpleExtension.ExtensionCollection collection,
        String resource
    ) {
        try (java.io.InputStream in = DataFusionFragmentConvertorTests.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + resource);
            }
            SimpleExtension.ExtensionCollection custom = SimpleExtension.load(in);
            return collection.merge(custom);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to load " + resource, e);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /** Builds a nullable row type with integer columns named "A", "B", ... */
    private RelDataType rowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    /** Decodes Substrait proto bytes into a {@link Plan}. */
    private Plan decodeSubstrait(byte[] bytes) throws Exception {
        assertNotNull("convertor bytes must not be null", bytes);
        assertTrue("convertor bytes must not be empty", bytes.length > 0);
        return Plan.parseFrom(bytes);
    }

    /** Extracts the single root {@link Rel} of a Substrait {@link Plan}. */
    private Rel rootRel(Plan plan) {
        assertFalse("plan must contain at least one relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        return planRel.getRoot().getInput();
    }

    /**
     * Builds a Calcite {@code LogicalTableScan} via the convertor's own
     * {@link DataFusionFragmentConvertor.StageInputTableScan} — a minimal TableScan
     * subclass that the isthmus visitor emits as a {@link ReadRel} with a
     * one-element named-table reference.
     */
    private RelNode buildTableScan(String tableName, String... columns) {
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), tableName, rowType(columns));
    }

    private LogicalAggregate buildSumAggregate(RelNode input, int columnIndex) {
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(columnIndex),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "sum_col"
        );
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * A bare table scan converts to a {@code ReadRel} whose named table carries
     * the supplied tableName (no catalog prefix).
     */
    public void testConvertShardScanFragment_TableScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        byte[] bytes = newConvertor().convertShardScanFragment("test_index", scan);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a ReadRel", root.hasRead());
        ReadRel read = root.getRead();
        assertTrue("ReadRel must reference a named table", read.hasNamedTable());
        assertEquals(List.of("test_index"), read.getNamedTable().getNamesList());
    }

    /**
     * A {@code Filter(Scan)} fragment converts to {@code FilterRel(ReadRel)}.
     */
    public void testConvertShardScanFragment_FilterOverScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(scan, predicate);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a FilterRel", root.hasFilter());
        FilterRel filterRel = root.getFilter();
        assertTrue("FilterRel must carry a condition", filterRel.hasCondition());
        Rel inner = filterRel.getInput();
        assertTrue("Filter input must be a ReadRel", inner.hasRead());
        assertEquals(List.of("test_index"), inner.getRead().getNamedTable().getNamesList());
    }

    /**
     * Attaching a partial aggregate on top of inner bytes yields an
     * {@code AggregateRel(readRel)} with phase INITIAL_TO_INTERMEDIATE.
     */
    public void testAttachPartialAggOnTop_WrapsInner() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner bytes from a shard-scan conversion.
        RelNode scan = buildTableScan("test_index", "A");
        byte[] innerBytes = convertor.convertShardScanFragment("test_index", scan);

        // Build a bare partial-agg fragment whose input matches the inner's rowType.
        LogicalAggregate partialAgg = buildSumAggregate(scan, 0);

        byte[] combined = convertor.attachPartialAggOnTop(partialAgg, innerBytes);

        Plan plan = decodeSubstrait(combined);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel agg = root.getAggregate();
        assertFalse("aggregate must have at least one measure", agg.getMeasuresList().isEmpty());
        AggregateFunction fn = agg.getMeasures(0).getMeasure();
        assertEquals(
            "partial-agg phase must be INITIAL_TO_INTERMEDIATE",
            AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
            fn.getPhase()
        );
        // Aggregate is rewired over the inner plan's root ReadRel — the partial-agg path does
        // not pin state columns, so no Project layer is interposed here.
        assertEquals("test_index", readTableName(agg.getInput()));
    }

    /**
     * A final-agg fragment whose leaf is an {@link OpenSearchStageInputScan} converts to
     * {@code AggregateRel(ProjectRel(ReadRel(namedTable=["input-0"])))} — pinStageInputStateColumns
     * inserts an identity Project above the StageInputScan so state columns survive Calcite's
     * unused-column pruning.
     */
    public void testConvertFinalAggFragment_WithStageInputScanLeaf() throws Exception {
        RelDataType stageRowType = rowType("A");
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 7, stageRowType, List.of("datafusion"));
        LogicalAggregate finalAgg = buildSumAggregate(stageInput, 0);

        byte[] bytes = newConvertor().convertFinalAggFragment(finalAgg);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel agg = root.getAggregate();
        assertFalse("aggregate must have at least one measure", agg.getMeasuresList().isEmpty());
        // Isthmus defaults final-mode aggregates to INITIAL_TO_RESULT.
        AggregateFunction fn = agg.getMeasures(0).getMeasure();
        assertEquals("final-agg phase must be INITIAL_TO_RESULT", AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT, fn.getPhase());
        assertEquals(DatafusionReduceSink.INPUT_ID, readTableName(agg.getInput()));
    }

    /**
     * Attaching a {@link LogicalSort} on top of inner bytes yields
     * {@code SortRel(<inner>)}.
     */
    public void testAttachFragmentOnTop_Sort() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner: final-agg over stage-input.
        RelDataType stageRowType = rowType("A");
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 3, stageRowType, List.of("datafusion"));
        LogicalAggregate finalAgg = buildSumAggregate(stageInput, 0);
        byte[] innerBytes = convertor.convertFinalAggFragment(finalAgg);

        // Contract: attachFragmentOnTop receives a childless operator. Sort requires an
        // input for row-type validation in the isthmus visitor; give it a bare placeholder
        // with the same output row type as the inner agg. The placeholder is discarded
        // during rewire (replaced with the inner plan's root).
        RelNode placeholderInput = buildTableScan("__placeholder__", "sum_col");
        LogicalSort sort = LogicalSort.create(placeholderInput, RelCollations.of(0), null, null);

        byte[] combined = convertor.attachFragmentOnTop(sort, innerBytes);

        Plan plan = decodeSubstrait(combined);
        Rel root = rootRel(plan);
        assertTrue("root must be a SortRel", root.hasSort());
        SortRel sortRel = root.getSort();
        // Sort is rewired over the inner agg.
        Rel inner = sortRel.getInput();
        assertTrue("Sort input must be an AggregateRel", inner.hasAggregate());
        assertEquals(DatafusionReduceSink.INPUT_ID, readTableName(inner.getAggregate().getInput()));
    }

    /**
     * Attaching a {@link LogicalSort} that carries only a {@code fetch} (no collation) on top
     * of inner bytes yields {@code FetchRel(<inner>)}. Substrait splits Calcite's single Sort
     * concept across two rel types: {@code SortRel} for collation, {@code FetchRel} for
     * limit+offset. Isthmus picks Fetch when the collation is empty and the fetch is set.
     *
     * <p>PPL emits a default {@code Sort(fetch=10000)} at the top of every query to cap the
     * result-set size; every coord-side fragment therefore ends up with a Fetch wrapper. The
     * rewire path must mirror the Sort case so the inner plan's root gets re-attached under
     * the Fetch instead of whatever placeholder the standalone conversion used.
     */
    public void testAttachFragmentOnTop_Fetch() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();

        // Inner: final-agg over stage-input (same shape as the Sort case).
        RelDataType stageRowType = rowType("A");
        RelNode stageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 5, stageRowType, List.of("datafusion"));
        LogicalAggregate finalAgg = buildSumAggregate(stageInput, 0);
        byte[] innerBytes = convertor.convertFinalAggFragment(finalAgg);

        // Wrapper: Sort with fetch set but no collation → isthmus emits substrait Fetch.
        RelNode placeholderInput = buildTableScan("__placeholder__", "sum_col");
        LogicalSort fetchOnly = LogicalSort.create(
            placeholderInput,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        byte[] combined = convertor.attachFragmentOnTop(fetchOnly, innerBytes);

        Plan plan = decodeSubstrait(combined);
        Rel root = rootRel(plan);
        assertTrue("root must be a FetchRel", root.hasFetch());
        FetchRel fetchRel = root.getFetch();
        Rel inner = fetchRel.getInput();
        assertTrue("Fetch input must be an AggregateRel", inner.hasAggregate());
        assertEquals(DatafusionReduceSink.INPUT_ID, readTableName(inner.getAggregate().getInput()));
    }

    /**
     * A coord-side join fragment with two {@link OpenSearchStageInputScan} leaves converts
     * to a {@code JoinRel} whose left and right inputs read from the dense
     * {@code "input-0"} / {@code "input-1"} table references — the same names the reduce
     * sink registers against the local DataFusion session.
     */
    public void testConvertJoinFragment_TwoStageInputScans_ProducesNamedScansInputZeroAndOne() throws Exception {
        // Synthesize the post-strip fragment: LogicalJoin(StageInputScan(left), StageInputScan(right)).
        // The DAG builder's child-stage ordering (left first, right second) determines the dense
        // index assignment performed by rewriteStageInputScans.
        RelDataType leftRowType = rowType("k", "v");
        RelDataType rightRowType = rowType("k", "w");
        RelNode leftStageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, leftRowType, List.of("datafusion"));
        RelNode rightStageInput = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 1, rightRowType, List.of("datafusion"));

        // Equi-join condition on left.k = right.k. Right field 0 is offset by left fieldCount=2.
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true), 0),
            rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true), 2)
        );
        LogicalJoin join = LogicalJoin.create(
            leftStageInput,
            rightStageInput,
            List.of(),
            condition,
            new HashSet<>(),
            JoinRelType.INNER
        );

        byte[] bytes = newConvertor().convertFinalAggFragment(join);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a JoinRel", root.hasJoin());
        JoinRel joinRel = root.getJoin();
        assertEquals(JoinRel.JoinType.JOIN_TYPE_INNER, joinRel.getType());

        // Each input is wrapped in a Project (pinStageInputStateColumns inserts an identity
        // Project above every StageInputScan). Walk through and assert the underlying ReadRel
        // table names line up with the dense "input-{i}" convention.
        assertEquals("input-0", readTableName(joinRel.getLeft()));
        assertEquals("input-1", readTableName(joinRel.getRight()));
    }

    /**
     * A scan with a {@code Project(SCALAR_MIN(a, 1, 2))} on top converts without throwing
     * {@code "Unable to convert call SCALAR_MIN(...)"}. Exercises the YAML pair:
     * {@code scalar_min → least} alias plus a {@code least} signature matching the 3-arg
     * shape PPL emits from its {@code ScalarMinFunction} UDF.
     */
    public void testConvertProject_ScalarMinThreeArgs_Resolves() throws Exception {
        assertUdfCallResolves("SCALAR_MIN");
    }

    /**
     * As above but for {@code SCALAR_MAX} / {@code greatest}.
     */
    public void testConvertProject_ScalarMaxThreeArgs_Resolves() throws Exception {
        assertUdfCallResolves("SCALAR_MAX");
    }

    /**
     * PPL's {@code array(...)} UDF is registered under the lowercase name {@code "array"}.
     * DataFusion's native list constructor is {@code make_array}. Exercises the YAML pair:
     * {@code array → make_array} alias plus a 5-arg {@code make_array} overload matching
     * {@code CalciteArrayFunctionIT}'s observed call shape.
     */
    public void testConvertProject_ArrayConstructorFiveArgs_Resolves() throws Exception {
        assertUdfCallResolves("array", 5);
    }

    private void assertUdfCallResolves(String udfName) throws Exception {
        assertUdfCallResolves(udfName, 3);
    }

    /**
     * Builds a minimal {@link org.apache.calcite.sql.SqlFunction} with {@code udfName}, wraps
     * it in a Project over a table scan with {@code arity} i32-nullable operands, and asserts
     * the convertor emits a ProjectRel without throwing the substrait resolver error.
     * <p>The UDF name is what the substrait emission layer ({@code NameBasedScalarFunctionConverter})
     * uses to look up the call in {@code opensearch_scalar.yaml} — matching the exact names
     * registered by the PPL frontend in the sql repo.
     */
    private void assertUdfCallResolves(String udfName, int arity) throws Exception {
        RelNode scan = buildTableScan("test_index", "a");
        RelDataType i32 = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        List<RexNode> operands = new java.util.ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            operands.add(rexBuilder.makeInputRef(scan, 0));
        }
        org.apache.calcite.sql.SqlFunction udf = new org.apache.calcite.sql.SqlFunction(
            udfName,
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.explicit(i32),
            null,
            org.apache.calcite.sql.type.OperandTypes.VARIADIC,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode call = rexBuilder.makeCall(udf, operands);
        RelNode project = LogicalProject.create(scan, List.of(), List.of(call), List.of("v"), Set.of());

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a ProjectRel", root.hasProject());
        assertFalse("project must carry at least one expression", root.getProject().getExpressionsList().isEmpty());
    }

    /** Walks through one optional ProjectRel layer to fetch the named-table reference of the leaf ReadRel. */
    private String readTableName(Rel rel) {
        if (rel.hasProject()) {
            ProjectRel project = rel.getProject();
            assertTrue("project input must be a ReadRel", project.getInput().hasRead());
            return project.getInput().getRead().getNamedTable().getNamesList().getFirst();
        }
        assertTrue("expected ReadRel or ProjectRel(ReadRel), got " + rel, rel.hasRead());
        return rel.getRead().getNamedTable().getNamesList().getFirst();
    }

    // ── Window-function dedup ──────────────────────────────────────────────────
    //
    // DataFusion's substrait consumer auto-names each WindowFunctionInvocation from its
    // canonical form (e.g. "count(Int64(1)) PARTITION BY [...] ROWS ..."). Two
    // invocations with identical signatures inside the same Project collide — the
    // consumer then fails schema validation with:
    //   "Schema contains duplicate unqualified field name ..."
    //
    // How this happens in practice (streamstats/eventstats):
    //   sql-plugin's PlanUtils.makeOver decomposes  avg(x) OVER (spec)
    //      into                                     sum(x) OVER (spec) / count(x) OVER (spec).
    //   Our CountWindowRewriter then strips the arg:  count(x) OVER (spec) → count() OVER (spec).
    //   If the user's query also contained a separate  count() OVER (spec),
    //   we now have TWO identical count() OVER (spec) RexOvers. DataFusion rejects.
    //
    // Same mechanism produces the ROW_NUMBER() collision in chained streamstats.

    /** Builds {@code count() OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)} — no partition,
     *  no order. The frame matches what streamstats default-emits. */
    private RexNode buildCountRowsUnboundedToCurrent() {
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        return rexBuilder.makeOver(
            bigint,
            SqlStdOperatorTable.COUNT,
            List.of(),
            List.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            /* rows= */ true,
            /* allowPartial= */ true,
            /* nullWhenCountZero= */ false,
            /* distinct= */ false,
            /* ignoreNulls= */ false
        );
    }

    /** Builds {@code count(col) OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)} — same frame,
     *  with one operand. {@link DataFusionFragmentConvertor}'s CountWindowRewriter collapses
     *  this to {@code count()} — colliding with {@link #buildCountRowsUnboundedToCurrent}. */
    private RexNode buildCountColRowsUnboundedToCurrent(RelNode scan, int colIdx) {
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        return rexBuilder.makeOver(
            bigint,
            SqlStdOperatorTable.COUNT,
            List.of(rexBuilder.makeInputRef(scan, colIdx)),
            List.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, true, false, false, false
        );
    }

    /** Returns every top-level {@link Expression.WindowFunction} (recursively, including inside
     *  any {@link Expression.Cast}) across all Project expressions in {@code project}. Wraps
     *  each in its proto byte form so identical invocations compare equal. */
    private List<byte[]> topLevelWindowFnBytes(ProjectRel project) {
        List<byte[]> out = new java.util.ArrayList<>();
        for (Expression expr : project.getExpressionsList()) {
            collectTopLevelWindows(expr, out);
        }
        return out;
    }

    private void collectTopLevelWindows(Expression expr, List<byte[]> out) {
        if (expr.hasWindowFunction()) {
            out.add(expr.getWindowFunction().toByteArray());
        } else if (expr.hasCast()) {
            collectTopLevelWindows(expr.getCast().getInput(), out);
        }
    }

    /**
     * If a Project contains two RexOvers that are textually identical (same op, same operands,
     * same partition / order / frame), the convertor must dedupe them before the substrait
     * plan reaches DataFusion — otherwise DataFusion rejects the schema with
     * {@code "Schema contains duplicate unqualified field name"}.
     */
    public void testConvertShardScanFragment_DuplicateWindowFunctionsDeduped() throws Exception {
        RelNode scan = buildTableScan("test_index", "a", "b");

        RexNode countStar1 = buildCountRowsUnboundedToCurrent();
        RexNode countStar2 = buildCountRowsUnboundedToCurrent();
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(countStar1, countStar2),
            List.of("c1", "c2"),
            Set.of()
        );

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a ProjectRel", root.hasProject());

        List<byte[]> windows = topLevelWindowFnBytes(root.getProject());
        long distinct = windows.stream().map(java.util.Arrays::hashCode).distinct().count();
        assertEquals(
            "expected at most one distinct top-level WindowFunction invocation after dedup; got "
                + windows.size() + " total, " + distinct + " distinct",
            windows.size(),
            distinct
        );
    }

    /**
     * Two RexOvers wrapped inside the same containing expression (so both are nested, not
     * top-level) must share a single lower-projection entry — otherwise the lifter would emit
     * two identical {@code count()} windows side by side and DataFusion would reject the plan.
     */
    public void testConvertShardScanFragment_NestedDuplicateWindows_ShareLowerProjectColumn() throws Exception {
        RelNode scan = buildTableScan("test_index", "a");

        RexNode win1 = buildCountRowsUnboundedToCurrent();
        RexNode win2 = buildCountRowsUnboundedToCurrent();
        // Wrap in an ADD so the RexOvers are nested, forcing the lifter's NestedWindowShuttle path.
        RelDataType bigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RexNode sumOfWindows = rexBuilder.makeCall(bigint, SqlStdOperatorTable.PLUS, List.of(win1, win2));
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(sumOfWindows),
            List.of("total"),
            Set.of()
        );

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        // Top-level is a Project (plus over two lower windows). The two RexOvers have the same
        // digest, so the lifter must push them to the SAME lower column — emitting only one
        // WindowFunction in the lower Project.
        assertTrue("root must be a ProjectRel", root.hasProject());
        ProjectRel outer = root.getProject();
        assertTrue("outer project input must be a lower Project holding the lifted windows",
            outer.getInput().hasProject());
        ProjectRel lower = outer.getInput().getProject();
        List<byte[]> lowerWindows = topLevelWindowFnBytes(lower);
        long distinct = lowerWindows.stream().map(java.util.Arrays::hashCode).distinct().count();
        assertEquals(
            "the lifter must dedupe identical nested RexOvers so the lower Project emits only"
                + " one WindowFunction; got " + lowerWindows.size() + " total, " + distinct + " distinct",
            lowerWindows.size(),
            distinct
        );
    }

    /**
     * The realistic streamstats/eventstats collision: {@code count()} and {@code count(col)} —
     * different at the Calcite level, but the CountWindowRewriter collapses {@code count(col)}
     * to {@code count()}, producing two identical RexOvers. The convertor must still emit a
     * non-colliding plan.
     */
    public void testConvertShardScanFragment_CountStarAndCountColCollapse_Deduped() throws Exception {
        RelNode scan = buildTableScan("test_index", "a", "b");

        RexNode countStar = buildCountRowsUnboundedToCurrent();
        RexNode countCol = buildCountColRowsUnboundedToCurrent(scan, 0);
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(countStar, countCol),
            List.of("cnt_star", "cnt_col"),
            Set.of()
        );

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be a ProjectRel", root.hasProject());

        List<byte[]> windows = topLevelWindowFnBytes(root.getProject());
        long distinct = windows.stream().map(java.util.Arrays::hashCode).distinct().count();
        assertEquals(
            "after count-arg stripping both invocations become count() OVER (same spec) — "
                + "the convertor must dedupe so the emitted plan has no colliding WindowFunctions; "
                + "got " + windows.size() + " total, " + distinct + " distinct",
            windows.size(),
            distinct
        );
    }
}
