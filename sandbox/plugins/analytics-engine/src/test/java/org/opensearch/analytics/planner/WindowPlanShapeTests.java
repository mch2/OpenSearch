/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

import java.util.List;

/**
 * Plan-shape tests for window functions, expressed as {@code RexOver} inside a
 * {@link LogicalProject} (the shape PPL {@code eventstats} / {@code appendcol} emit).
 *
 * <p>The planner detects RexOver inside {@code OpenSearchProjectRule}, narrows viable
 * backends by {@link org.opensearch.analytics.spi.WindowCapability}, and applies a cost
 * gate that requires {@code COORDINATOR+SINGLETON} input on the project when any
 * expression is a RexOver.
 */
public class WindowPlanShapeTests extends PlanShapeTestBase {

    /**
     * 1-shard with empty OVER(). Project's RexOver cost gate forces COORDINATOR — but
     * the input is already on one node. Today the planner inserts an ER under the
     * Project; the optimization is to keep the Project at SHARD and ER above instead.
     */
    @AwaitsFix(bugUrl = "Optimization: RexOver Project over 1-shard SHARD+SINGLETON should stay at shard.")
    public void testCountOverEmpty_1shard() {
        RelNode plan = projectWithCountOverEmpty();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testCountOverEmpty_2shard() {
        RelNode plan = projectWithCountOverEmpty();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testSumOverEmpty_2shard() {
        RelNode plan = projectWithSumOverEmpty();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a shard-side Filter (multi-shard). Filter is single-input passthrough,
     * stays at SHARD; the RexOver Project's cost gate forces COORDINATOR input, so an ER
     * sits between Filter and Project.
     */
    public void testSumOverEmpty_afterFilter_2shard() {
        RelNode plan = projectWithSumOverEmpty(makeFilter(stubScan(mockTable("test_index", "status", "size")),
            makeEquals(0, SqlTypeName.INTEGER, 200)));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a Filter on a 1-shard input. Same suboptimality as
     * {@link #testCountOverEmpty_1shard}: the Project's RexOver cost gate forces
     * COORDINATOR and an ER lands under it even though the input subtree is already
     * single-node.
     */
    @AwaitsFix(bugUrl = "Optimization: RexOver Project over 1-shard SHARD+SINGLETON should stay at shard.")
    public void testSumOverEmpty_afterFilter_1shard() {
        RelNode plan = projectWithSumOverEmpty(makeFilter(stubScan(mockTable("test_index", "status", "size")),
            makeEquals(0, SqlTypeName.INTEGER, 200)));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a collated Sort (multi-shard). Sort already gathers to SINGLETON via
     * SortSplitRule, so the Project's RexOver cost gate is satisfied without a second ER —
     * Sort's output IS the SINGLETON the Project demands.
     */
    public void testSumOverEmpty_afterSort_2shard() {
        RelNode plan = projectWithSumOverEmpty(makeSort(stubScan(mockTable("test_index", "status", "size")), -1));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Two RexOver expressions in the same Project — both empty OVER() — share one ER.
     * Verifies the rule emits exactly one OpenSearchProject wrapping both windows and
     * does not duplicate the ER per RexOver.
     */
    public void testMultipleRexOver_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode countOver = makeOver(rb, scan, SqlStdOperatorTable.COUNT, List.of(), SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING);
        RexNode sumOver = makeOver(rb, scan, SqlStdOperatorTable.SUM, List.of(rb.makeInputRef(scan, 1)), SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING);

        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), countOver, sumOver),
            List.of("status", "size", "cnt", "s")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Non-default frame: SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) —
     * a running aggregate. The frame bounds change the OVER() rendering but not the plan
     * shape (still ER under Project, same backend narrowing). Today no PPL command on this
     * route emits this frame ({@code streamstats} isn't wired here), but the planner must
     * still handle it correctly because Calcite can construct it directly.
     */
    public void testSumOverRunningFrame_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode runningSum = makeOver(rb, scan, SqlStdOperatorTable.SUM, List.of(rb.makeInputRef(scan, 1)), SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW);

        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), runningSum),
            List.of("status", "size", "running_s")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], running_s=[SUM($1) OVER (ROWS UNBOUNDED PRECEDING)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * OVER (PARTITION BY ...) is rejected at marking time — no shuffle exchange yet.
     */
    public void testRexOverPartitionBy_rejected() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode over = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            ImmutableList.of(rb.makeInputRef(scan, 0)),    // PARTITION BY status
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("status", "size", "s")
        );
        try {
            runPlanner(plan, multiShardContext());
            fail("Expected planner to reject PARTITION BY");
        } catch (RuntimeException expected) {
            // OK — rule rejected the RexOver.
        }
    }

    // ── Builders ──────────────────────────────────────────────────────────

    private RelNode projectWithCountOverEmpty() {
        return buildProjectWithOver(SqlStdOperatorTable.COUNT, List.of(), "cnt", SqlTypeName.BIGINT);
    }

    private RelNode projectWithSumOverEmpty() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        return projectWithSumOverEmpty(scan);
    }

    /** Same as {@link #projectWithSumOverEmpty()} but stacked on a caller-supplied input
     *  (Filter, Sort, etc.). Input must have at least 2 fields ($0 status, $1 size). */
    private RelNode projectWithSumOverEmpty(RelNode input) {
        RexBuilder rb = input.getCluster().getRexBuilder();
        RexNode over = makeOver(rb, input, SqlStdOperatorTable.SUM, List.of(rb.makeInputRef(input, 1)),
            SqlTypeName.BIGINT, RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING);
        return LogicalProject.create(
            input,
            List.of(),
            List.of(rb.makeInputRef(input, 0), rb.makeInputRef(input, 1), over),
            List.of("status", "size", "s")
        );
    }

    /** Build a {@code RexOver} with the given function, operands, output type, and frame bounds.
     *  Empty PARTITION BY and empty ORDER BY. */
    private RexNode makeOver(
        RexBuilder rb,
        RelNode scan,
        SqlAggFunction fn,
        List<RexNode> operands,
        SqlTypeName outType,
        RexWindowBound lowerBound,
        RexWindowBound upperBound
    ) {
        return rb.makeOver(
            typeFactory.createSqlType(outType),
            fn,
            operands,
            ImmutableList.of(),
            ImmutableList.of(),
            lowerBound,
            upperBound,
            true,
            true,
            false,
            false,
            false
        );
    }

    /**
     * Build a LogicalProject that passes through every scan column and adds one RexOver
     * (empty OVER clause) as the final projected expression.
     */
    private RelNode buildProjectWithOver(SqlAggFunction fn, List<RexNode> operands, String outName, SqlTypeName outType) {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode over = rb.makeOver(
            typeFactory.createSqlType(outType),
            fn,
            operands,
            ImmutableList.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );

        return LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("status", "size", outName)
        );
    }
}
