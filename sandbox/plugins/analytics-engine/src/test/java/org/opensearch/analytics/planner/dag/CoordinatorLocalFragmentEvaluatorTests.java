/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchValues;

import java.util.Iterator;
import java.util.List;

/**
 * Regression for testSumEmpty — {@code source=... | where 1=2 | stats sum(balance)}.
 *
 * <p>Task #9's marking + PruneEmpty/AggregateValues rules collapse that shape to
 * a literal-projection over a single-row {@code OpenSearchValues} node. Since
 * the plan is fully constant-folded, dispatching it to a shard is not meaningful;
 * {@link DAGBuilder#build} would fail because the fragment has no
 * {@code OpenSearchTableScan}. Instead, {@code DefaultPlanExecutor} short-circuits
 * via {@link CoordinatorLocalFragmentEvaluator} and returns the constant rows
 * directly.
 */
public class CoordinatorLocalFragmentEvaluatorTests extends BasePlannerRulesTests {

    /** testSumEmpty shape: {@code Project(0:BIGINT) over Values([[{null}]])} emits one row with value 0L. */
    public void testSumEmptyShapeProducesSingleNullRow() {
        RelNode plan = runThroughPlanner(buildAggregateOverEmpty());
        logger.info("Plan:\n{}", RelOptUtil.toString(plan));

        assertTrue(CoordinatorLocalFragmentEvaluator.isScanLessCoordinatorFragment(plan));
        Iterable<Object[]> rows = CoordinatorLocalFragmentEvaluator.evaluate(plan);
        Iterator<Object[]> it = rows.iterator();
        assertTrue("expected one row", it.hasNext());
        Object[] row = it.next();
        assertEquals("expected one column", 1, row.length);
        assertNull("SUM over empty folds to NULL by Calcite's AGGREGATE_VALUES", row[0]);
        assertFalse("expected exactly one row", it.hasNext());
    }

    public void testIsScanLessReturnsTrueForValuesRoot() {
        RelNode plan = runThroughPlanner(buildAggregateOverEmpty());
        assertTrue(CoordinatorLocalFragmentEvaluator.isScanLessCoordinatorFragment(plan));
    }

    /** A classic shard-scan plan must NOT be routed to the coordinator-local short-circuit. */
    public void testIsScanLessReturnsFalseForTableScanRoot() {
        RelNode plan = runThroughPlanner(stubScan(mockTable("test_index", "status", "size")));
        assertFalse(CoordinatorLocalFragmentEvaluator.isScanLessCoordinatorFragment(plan));
    }

    /**
     * A plan whose Project holds a RexCall (not a literal or input-ref) must NOT be
     * routed to the coordinator-local short-circuit — isScanLess returns false. This
     * guardrails against silently accepting plans the marking rules should have
     * constant-folded but didn't.
     */
    public void testProjectWithRexCallIsNotScanLess() {
        // Build Project(CAST($0)) over OpenSearchValues directly, bypassing the
        // planner — the pre-marking ReduceExpressionsRule would otherwise fold the
        // CAST into a literal, defeating the test.
        RelDataType rowType = typeFactory.builder().add("x", typeFactory.createSqlType(SqlTypeName.BIGINT)).build();
        OpenSearchValues values = new OpenSearchValues(
            cluster,
            cluster.traitSet(),
            rowType,
            ImmutableList.of(ImmutableList.of(rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BIGINT)))),
            List.of("mock-parquet")
        );
        RexNode castCall = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), rexBuilder.makeInputRef(values, 0));
        OpenSearchProject project = new OpenSearchProject(
            cluster,
            cluster.traitSet(),
            values,
            List.of(castCall),
            rowType,
            List.of("mock-parquet")
        );
        assertFalse(
            "Project with RexCall must not be classified as scan-less coordinator fragment",
            CoordinatorLocalFragmentEvaluator.isScanLessCoordinatorFragment(project)
        );
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /**
     * Builds the pre-optimization shape for {@code source=... | where 1=2 | stats sum}:
     * an {@link LogicalAggregate} with a {@code SUM} over a zero-row Values. The Hep
     * planner's AGGREGATE_VALUES + OpenSearchValuesRule combination collapses and marks
     * this into a single-row {@code OpenSearchValues} carrying the aggregate identity.
     */
    private RelNode buildAggregateOverEmpty() {
        RelDataType rowType = typeFactory.builder()
            .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("size", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
        LogicalValues empty = LogicalValues.create(cluster, rowType, ImmutableList.<ImmutableList<RexLiteral>>of());
        // SUM over empty is nullable — must declare the agg-call return type as such so
        // the AGGREGATE_VALUES rule can produce a nullable literal in the collapsed Values.
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            empty,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "total_size"
        );
        return LogicalAggregate.create(empty, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));
    }

    /** Returns the full CBO output (with the OpenSearchExchangeReducer wrapper intact) —
     *  matches what {@code DefaultPlanExecutor} hands to the evaluator in production. */
    private RelNode runThroughPlanner(RelNode input) {
        var context = buildContext("parquet", intFields());
        return runPlanner(input, context);
    }
}
