/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateTypeResolver;

import java.util.List;

/**
 * Off-cluster partial-aggregate lowering matrix. Drives the REAL marking → PARTIAL/FINAL
 * split → CBO → DAG path ({@link BasePlannerRulesTests#runPlanner}) with the mock backends —
 * no node, no native lib, and (the point of the option-2 refactor) <b>no Arrow on the
 * classpath</b>: {@link AggregateFunction}'s enum {@code <clinit>} no longer references
 * {@code org.apache.arrow.ArrowType}, so touching an aggregate constant here never forces
 * Arrow to load. Partial-aggregate lowering — the split shape AND the intermediate-field type
 * resolution that {@code FinalAggCallBuilder}/{@code DistributedAggregateRewriter} consume — is
 * therefore unit-testable off-cluster.
 *
 * <p>Two axes:
 * <ul>
 *   <li><b>Split-vs-gather</b>: {@code SIMPLE}/{@code APPROXIMATE} aggregates split into
 *       PARTIAL/FINAL across shards; {@code STATE_EXPANDING} aggregates skip the split and
 *       gather to a single coordinator stage
 *       ({@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit}).</li>
 *   <li><b>Intermediate-field type resolution</b>: the two fixed-type decompositions — COUNT
 *       ({@code INT64} → BIGINT) and APPROX_COUNT_DISTINCT ({@code BINARY} → VARBINARY) — must
 *       resolve to the type the Arrow-free {@link AggregateFunction.FixedType} descriptor
 *       produces. This asserts the descriptor and the lowering agree without an Arrow round-trip.</li>
 * </ul>
 *
 * <p>Plan-string shapes for SUM/COUNT/AVG/APPROX_COUNT_DISTINCT are covered exhaustively by
 * {@link AggregatePlanShapeTests}; this class adds the resolver contract and the full split-vs-
 * gather sweep. The <b>no-Arrow-on-the-classpath</b> proof itself lives in the frontend suite
 * ({@code analytics-sql-frontend}), which runs the parse → capability-walk → aggregate-resolution
 * path with no Arrow test dependency at all; this module keeps Arrow only for {@code exec/}
 * (vectors/allocators), which off-cluster lowering never touches.
 */
public class PartialAggregateLoweringMatrixTests extends PlanShapeTestBase {

    // ── Split-vs-gather sweep across the aggregate surface ──────────────────────────────────

    /** Multi-shard SUM splits into PARTIAL/FINAL with a reducing exchange between. */
    public void testSumSplitsPartialFinal() {
        assertSplits(sumCall(scan()), "SUM");
    }

    /** Multi-shard COUNT(*) splits; FINAL's COUNT is rebuilt as SUM over the partial counts. */
    public void testCountStarSplitsPartialFinal() {
        RelNode result = runPlanner(makeAggregate(scan(), countStarCall(scan())), multiShardContext());
        OpenSearchAggregate finalAgg = requireFinal(result, "COUNT(*)");
        // COUNT → SUM swap on FINAL is the decomposition FinalAggCallBuilder applies via the
        // COUNT intermediate field (reducer = SUM). Prove FINAL no longer carries a raw COUNT.
        assertEquals("FINAL must carry exactly one agg call", 1, finalAgg.getAggCallList().size());
        assertEquals(
            "COUNT's FINAL reducer must be SUM (function-swap decomposition)",
            org.apache.calcite.sql.SqlKind.SUM,
            finalAgg.getAggCallList().get(0).getAggregation().getKind()
        );
    }

    /** Multi-shard COUNT(DISTINCT) → APPROX_COUNT_DISTINCT splits, keeping the operator on FINAL (engine-native merge). */
    public void testApproxCountDistinctSplitsKeepingOperator() {
        RelNode result = runPlanner(makeAggregate(scan(), countDistinctCall(scan())), multiShardContext());
        OpenSearchAggregate finalAgg = requireFinal(result, "APPROX_COUNT_DISTINCT");
        assertEquals(
            "engine-native merge keeps APPROX_COUNT_DISTINCT on FINAL (reducer == self)",
            SqlStdName.APPROX_COUNT_DISTINCT,
            finalAgg.getAggCallList().get(0).getAggregation().getName()
        );
    }

    /** AVG decomposes to SUM/COUNT primitives PRE-split; the split then runs on those primitives (no AVG on PARTIAL). */
    public void testAvgDecomposesThenSplits() {
        RelNode result = runPlanner(makeAggregate(scan(), ImmutableBitSet.of(0), avgCall(scan())), multiShardContext());
        OpenSearchAggregate finalAgg = requireFinal(result, "AVG");
        // AVG is reduced to SUM/SUM on FINAL (the quotient is a Project above); no AVG survives here.
        for (AggregateCall call : finalAgg.getAggCallList()) {
            assertNotSame(
                "AVG must be reduced to primitives before FINAL — no raw AVG on the FINAL aggregate",
                org.apache.calcite.sql.SqlKind.AVG,
                call.getAggregation().getKind()
            );
        }
    }

    /** STATE_EXPANDING (PERCENTILE_APPROX) skips the split and gathers to a single coordinator stage. */
    public void testStateExpandingGathersInsteadOfSplitting() {
        AggregateCall percentile = percentileApproxCall(scan());
        RelNode result = runPlanner(makeAggregate(scan(), ImmutableBitSet.of(0), percentile), multiShardContext());
        // No FINAL is produced — the aggregate runs SINGLE after a singleton gather. Assert there is
        // no PARTIAL/FINAL pair anywhere in the tree.
        assertNoPartialFinal(result, "PERCENTILE_APPROX");
    }

    // ── Intermediate-field type resolution: descriptor ↔ lowering agreement (Arrow-free) ────

    /**
     * COUNT's intermediate field resolves to BIGINT through the Arrow-free {@code FixedType.INT64}
     * descriptor — the same resolver the split's {@code FinalAggCallBuilder} consumes. Uses a plain
     * {@link JavaTypeFactoryImpl} (no cluster, no Arrow) to run the resolver directly.
     */
    public void testCountIntermediateFieldResolvesToBigintArrowFree() {
        IntermediateField field = single(AggregateFunction.COUNT);
        assertEquals("count", field.name());
        assertSame("COUNT decomposes via a SUM reducer (function-swap)", AggregateFunction.SUM, field.reducer());
        assertEquals(SqlTypeName.BIGINT, resolve(field).getSqlTypeName());
    }

    /**
     * APPROX_COUNT_DISTINCT's intermediate field resolves to VARBINARY (HLL sketch state) through
     * the Arrow-free {@code FixedType.BINARY} descriptor, and is engine-native (reducer == self).
     */
    public void testApproxCountDistinctIntermediateFieldResolvesToVarbinaryArrowFree() {
        IntermediateField field = single(AggregateFunction.APPROX_COUNT_DISTINCT);
        assertEquals("hll_registers", field.name());
        assertSame("engine-native merge: reducer is the function itself", AggregateFunction.APPROX_COUNT_DISTINCT, field.reducer());
        assertEquals(SqlTypeName.VARBINARY, resolve(field).getSqlTypeName());
    }

    /** SIMPLE pass-through aggregates carry no intermediate-field decomposition (FINAL reuses the primitive). */
    public void testSimpleAggregatesHaveNoFixedIntermediate() {
        assertNull(AggregateFunction.SUM.intermediateFields());
        assertNull(AggregateFunction.MIN.intermediateFields());
        assertNull(AggregateFunction.MAX.intermediateFields());
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────────────

    private static final class SqlStdName {
        static final String APPROX_COUNT_DISTINCT = org.apache.calcite.sql.fun.SqlStdOperatorTable.APPROX_COUNT_DISTINCT.getName();
    }

    private RelNode scan() {
        return stubScan(mockTable("test_index", "status", "size"));
    }

    private AggregateCall percentileApproxCall(RelNode input) {
        // PERCENTILE_APPROX is a USER_DEFINED aggregate (SqlKind.OTHER) — model it with a UDAF
        // whose name resolves to AggregateFunction.PERCENTILE_APPROX by name.
        org.apache.calcite.sql.SqlAggFunction udaf = new org.apache.calcite.sql.SqlAggFunction(
            "PERCENTILE_APPROX",
            null,
            org.apache.calcite.sql.SqlKind.OTHER,
            org.apache.calcite.sql.type.ReturnTypes.ARG0_FORCE_NULLABLE,
            null,
            org.apache.calcite.sql.type.OperandTypes.NUMERIC,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            org.apache.calcite.util.Optionality.FORBIDDEN
        ) {
        };
        return AggregateCall.create(
            udaf,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            1,
            input,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "pct"
        );
    }

    /** Runs the multi-shard planner and asserts a PARTIAL/FINAL split was produced. */
    private void assertSplits(AggregateCall aggCall, String label) {
        RelNode result = runPlanner(makeAggregate(scan(), aggCall), multiShardContext());
        requireFinal(result, label);
        assertTrue(label + " must produce a PARTIAL stage beneath FINAL", hasMode(result, AggregateMode.PARTIAL));
    }

    /** Finds the FINAL OpenSearchAggregate in the tree, failing if the split did not run. */
    private OpenSearchAggregate requireFinal(RelNode root, String label) {
        OpenSearchAggregate finalAgg = findAggregate(root, AggregateMode.FINAL);
        assertNotNull(label + " must lower to a FINAL aggregate (PARTIAL/FINAL split), tree:\n" + explain(root), finalAgg);
        return finalAgg;
    }

    private void assertNoPartialFinal(RelNode root, String label) {
        assertNull(
            label + " must NOT produce a FINAL aggregate (state-expanding gathers), tree:\n" + explain(root),
            findAggregate(root, AggregateMode.FINAL)
        );
        assertFalse(label + " must NOT produce a PARTIAL aggregate", hasMode(root, AggregateMode.PARTIAL));
    }

    private static boolean hasMode(RelNode root, AggregateMode mode) {
        return findAggregate(root, mode) != null;
    }

    private static OpenSearchAggregate findAggregate(RelNode node, AggregateMode mode) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(node);
        if (unwrapped instanceof OpenSearchAggregate agg && agg.getMode() == mode) {
            return agg;
        }
        for (RelNode input : unwrapped.getInputs()) {
            OpenSearchAggregate found = findAggregate(input, mode);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static String explain(RelNode root) {
        return org.apache.calcite.plan.RelOptUtil.toString(root);
    }

    // -- Arrow-free resolver invocation --

    private final RelDataTypeFactory plainFactory = new JavaTypeFactoryImpl();
    private final RelDataType integer = plainFactory.createSqlType(SqlTypeName.INTEGER);

    private static IntermediateField single(AggregateFunction fn) {
        List<IntermediateField> fields = fn.intermediateFields();
        assertNotNull(fn + " must declare an intermediate field", fields);
        assertEquals(fn + " expected a single intermediate field", 1, fields.size());
        return fields.get(0);
    }

    private RelDataType resolve(IntermediateField field) {
        IntermediateTypeResolver resolver = field.typeResolver();
        return resolver.resolve(List.of(integer), plainFactory);
    }
}
