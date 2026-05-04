/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.planner.rules.OpenSearchValuesRule;

import java.util.List;

/**
 * Regression for testSumEmpty — {@code PPL | where 1=2 | stats sum(balance)}.
 *
 * <p>Calcite's {@link org.apache.calcite.rel.rules.ReduceExpressionsRule.FilterReduceExpressionsRule}
 * collapses {@code Filter(false)} to a {@link LogicalValues} empty-relation. The marking
 * rules must convert that to an {@link OpenSearchValues} so parent Aggregate/Project rules
 * see a marked {@link OpenSearchRelNode} child; otherwise they throw
 * {@code "... rule encountered unmarked child [LogicalValues]"}.
 */
public class ValuesRuleTests extends BasePlannerRulesTests {

    /** Empty Values with an int row type converts to OpenSearchValues marked with scan-capable backends. */
    public void testEmptyValuesMarkedAsOpenSearchValues() {
        PlannerContext context = buildContext("parquet", intFields());
        RelNode empty = emptyValues(
            typeFactory.builder()
                .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("size", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build()
        );
        RelNode result = unwrapExchange(runPlanner(empty, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        assertTrue("Root should be OpenSearchValues but was " + result.getClass().getSimpleName(), result instanceof OpenSearchValues);
        OpenSearchValues values = (OpenSearchValues) result;
        assertTrue(
            "OpenSearchValues viableBackends must include scan-capable backends but was " + values.getViableBackends(),
            values.getViableBackends().contains(MockDataFusionBackend.NAME)
        );
    }

    /**
     * Grand-total Aggregate over an empty LogicalValues collapses to a single-row
     * Values with the aggregate identity (SUM→NULL) via {@code AggregateValuesRule}.
     * The collapsed Values then goes through {@link OpenSearchValuesRule} — no
     * "unmarked child" exception. This is the post-{@code WHERE 1=2} shape.
     */
    public void testAggregateOverEmptyValuesCollapsesAndMarks() {
        PlannerContext context = buildContext("parquet", intFields());
        RelNode empty = emptyValues(
            typeFactory.builder()
                .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("size", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build()
        );
        // SUM over an empty relation is nullable — Calcite's type inference returns
        // a nullable INTEGER. Declaring NOT NULL would fail the type-mismatch assertion
        // inside the aggregate rule.
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            empty,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "total_size"
        );
        LogicalAggregate agg = LogicalAggregate.create(empty, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        RelNode result = unwrapExchange(runPlanner(agg, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        // AggregateValuesRule collapses Aggregate(Values[empty]) → Values(tuples=[[NULL]]),
        // which the marking rule then wraps as OpenSearchValues. No aggregate remains.
        assertTrue(
            "Root should be OpenSearchValues (aggregate collapsed) but was " + result.getClass().getSimpleName(),
            result instanceof OpenSearchValues
        );
        OpenSearchValues values = (OpenSearchValues) result;
        assertTrue(
            "OpenSearchValues viableBackends must include scan-capable backends but was " + values.getViableBackends(),
            values.getViableBackends().contains(MockDataFusionBackend.NAME)
        );
    }

    private LogicalValues emptyValues(RelDataType rowType) {
        return LogicalValues.create(cluster, rowType, ImmutableList.<ImmutableList<RexLiteral>>of());
    }
}
