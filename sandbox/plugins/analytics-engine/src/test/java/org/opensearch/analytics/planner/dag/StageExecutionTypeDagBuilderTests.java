/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;

import java.util.List;
import java.util.Map;

/**
 * Tests that {@link DAGBuilder} correctly marks root stages with the
 * appropriate {@link StageExecutionType} based on the fragment structure.
 *
 * <p>Validates: Requirements 1.3, 1.4, 1.5, 6.1, 6.2, 6.3, 6.7
 */
public class StageExecutionTypeDagBuilderTests extends BasePlannerRulesTests {

    private static final Map<String, Map<String, Object>> FIELDS = Map.of("A", Map.of("type", "integer"), "B", Map.of("type", "integer"));

    // ---- 3.1: SELECT * FROM T with 1 shard -> root is LOCAL ----

    public void testSingleShardScanIsLocal() {
        QueryDAG dag = buildDAG(1, stubScan(testTable()));

        Stage root = dag.rootStage();
        assertEquals("Single-shard scan root should be LOCAL", StageExecutionType.LOCAL, root.getExecutionType());
    }

    // ---- 3.2: SELECT * FROM T with 10 shards -> root is LOCAL ----

    public void testMultiShardScanIsLocal() {
        QueryDAG dag = buildDAG(10, stubScan(testTable()));

        Stage root = dag.rootStage();
        assertEquals("Multi-shard scan root should be LOCAL", StageExecutionType.LOCAL, root.getExecutionType());
    }

    // ---- 3.3: SELECT sum(A) FROM T -> root is LOCAL ----

    public void testFinalAggregateIsLocal() {
        RelNode aggregate = scalarAggregate(sumCallOnA());

        QueryDAG dag = buildDAG(10, aggregate);

        Stage root = dag.rootStage();
        assertEquals("Final aggregate root should be LOCAL", StageExecutionType.LOCAL, root.getExecutionType());
    }

    // ---- 3.4: SELECT B, sum(A) FROM T GROUP BY B -> root is LOCAL ----

    public void testGroupByIsLocal() {
        RelNode aggregate = groupByAggregate(ImmutableBitSet.of(1), sumCallOnA(false));

        QueryDAG dag = buildDAG(10, aggregate);

        Stage root = dag.rootStage();
        assertEquals("Group-by aggregate root should be LOCAL", StageExecutionType.LOCAL, root.getExecutionType());
    }

    // ---- 3.5: SELECT B FROM T GROUP BY B HAVING sum(A) > 1000 -> root is LOCAL ----

    public void testHavingIsLocal() {
        RelNode aggregate = groupByAggregate(ImmutableBitSet.of(1), sumCallOnA(false));

        // HAVING sum(A) > 1000 -- filter above aggregate
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelNode filter = LogicalFilter.create(
            aggregate,
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(intType, 1),
                rexBuilder.makeLiteral(1000, intType, true)
            )
        );

        QueryDAG dag = buildDAG(10, filter);

        Stage root = dag.rootStage();
        assertEquals("HAVING clause root should be LOCAL", StageExecutionType.LOCAL, root.getExecutionType());
    }

    // ---- 3.6: Child stages of LOCAL root have stageIds mappable to __stage_N_input__ ----

    public void testStageInputIdsAssigned() {
        RelNode aggregate = scalarAggregate(sumCallOnA());

        QueryDAG dag = buildDAG(10, aggregate);

        Stage root = dag.rootStage();
        assertEquals("Root should be LOCAL for this aggregate query", StageExecutionType.LOCAL, root.getExecutionType());

        // Verify child stages exist and their IDs are mappable to __stage_N_input__
        assertFalse("LOCAL root should have child stages", root.getChildStages().isEmpty());
        for (Stage child : root.getChildStages()) {
            int childId = child.getStageId();
            String expectedInputId = "__stage_" + childId + "_input__";
            assertTrue("Stage input ID should be derivable from child stageId " + childId, expectedInputId.matches("__stage_\\d+_input__"));
            assertEquals("Child stage " + childId + " should be DATA_NODE", StageExecutionType.DATA_NODE, child.getExecutionType());
        }
    }

    // ---- Helpers ----

    private RelOptTable testTable() {
        return mockTable("test_index", new String[] { "A", "B" }, new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.INTEGER });
    }

    private AggregateCall sumCallOnA() {
        return sumCallOnA(true);
    }

    private AggregateCall sumCallOnA(boolean nullable) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), nullable),
            "sum_A"
        );
    }

    private RelNode scalarAggregate(AggregateCall aggCall) {
        return LogicalAggregate.create(stubScan(testTable()), List.of(), ImmutableBitSet.of(), null, List.of(aggCall));
    }

    private RelNode groupByAggregate(ImmutableBitSet groupSet, AggregateCall aggCall) {
        return LogicalAggregate.create(stubScan(testTable()), List.of(), groupSet, null, List.of(aggCall));
    }

    private QueryDAG buildDAG(int shardCount, RelNode logicalPlan) {
        PlannerContext context = buildContext("parquet", shardCount, FIELDS);
        RelNode cboOutput = runPlanner(logicalPlan, context);
        return DAGBuilder.build(cboOutput);
    }
}
