/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Plan-shape assertions for window functions expressed as RexOver inside a LogicalProject
 * (the shape PPL {@code eventstats} / {@code appendcol} emit). The planner detects RexOver
 * inside OpenSearchProjectRule, narrows viable backends by WindowCapability, and applies a
 * cost gate that forces SINGLETON input on the project when any expression is a RexOver.
 */
public class WindowPlanShapeTests extends BasePlannerRulesTests {

    private PlannerContext multiShardContext() {
        return buildContext(
            "parquet",
            3,
            Map.of("k", Map.of("type", "integer"), "v", Map.of("type", "integer"))
        );
    }

    private PlannerContext singleShardContext() {
        return buildContext(
            "parquet",
            1,
            Map.of("k", Map.of("type", "integer"), "v", Map.of("type", "integer"))
        );
    }

    /** Project with COUNT() OVER () on multi-shard — ER inserted under the project to gather. */
    public void testCountOverEmpty_multiShard() {
        RelNode plan = projectWithCountOverEmpty();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$0], v=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Single-shard is SINGLETON(SCAN) — satisfies the project's cost gate directly, no ER. */
    public void testCountOverEmpty_singleShard_noER() {
        RelNode plan = projectWithCountOverEmpty();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$0], v=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** SUM(v) OVER () — aggregate-as-window; same cost gate applies. */
    public void testSumOverEmpty_multiShard() {
        RelNode plan = projectWithSumOverEmpty();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$0], v=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Builders ──────────────────────────────────────────────────────────

    private RelNode projectWithCountOverEmpty() {
        return buildProjectWithOver(SqlStdOperatorTable.COUNT, List.of(), "cnt", SqlTypeName.BIGINT);
    }

    private RelNode projectWithSumOverEmpty() {
        RelOptTable table = mockTable("test_index", "k", "v");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        return buildProjectWithOver(
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            "s",
            SqlTypeName.BIGINT
        );
    }

    /**
     * Build a LogicalProject that passes through every scan column and adds one RexOver
     * (empty OVER clause) as the final projected expression.
     */
    private RelNode buildProjectWithOver(SqlAggFunction fn, List<RexNode> operands, String outName, SqlTypeName outType) {
        RelOptTable table = mockTable("test_index", "k", "v");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode over = rb.makeOver(
            typeFactory.createSqlType(outType),
            fn,
            operands,
            ImmutableList.of(),                  // partition keys (empty)
            ImmutableList.of(),                  // order keys (empty)
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,                                // physical (rows) vs logical (range)
            true,                                // allowPartial
            false,                               // nullWhenCountZero
            false,                               // distinct
            false                                // ignoreNulls
        );

        return LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("k", "v", outName)
        );
    }

    // ── Assertion helpers ──────────────────────────────────────────────────

    private static void assertPlanShape(String expected, RelNode actual) {
        String actualStr = RelOptUtil.toString(actual);
        String normalizedExpected = normalizeLines(expected);
        String normalizedActual = normalizeLines(actualStr);
        assertEquals("Plan shape mismatch — actual:\n" + actualStr, normalizedExpected, normalizedActual);
    }

    private static String normalizeLines(String s) {
        StringBuilder sb = new StringBuilder();
        for (String line : s.split("\n", -1)) {
            int end = line.length();
            while (end > 0 && (line.charAt(end - 1) == ' ' || line.charAt(end - 1) == '\t')) end--;
            sb.append(line, 0, end).append('\n');
        }
        while (sb.length() >= 2 && sb.charAt(sb.length() - 1) == '\n' && sb.charAt(sb.length() - 2) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
}
