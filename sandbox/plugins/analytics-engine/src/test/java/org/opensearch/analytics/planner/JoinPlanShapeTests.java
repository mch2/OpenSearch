/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchJoin}.
 *
 * <p>Two execution strategies, both via {@code OpenSearchJoinSplitRule}:
 * <ul>
 *   <li><b>Co-location fast path</b> — both sides are 1-shard scans of the same table.
 *       Whole subtree resolves at the shard, no per-side ER. Output is
 *       {@code SHARD+SINGLETON+t=X+s=1}; root demand triggers a single ER above.</li>
 *   <li><b>General path</b> — different tables or any non-1-shard input. Each side is
 *       gathered to {@code COORDINATOR+SINGLETON} via {@code TraitDef.convert}. ER per
 *       side, Join runs at coord.</li>
 * </ul>
 */
public class JoinPlanShapeTests extends PlanShapeTestBase {

    public void testInnerJoin_selfJoin_1shard() {
        // Self-join on a single 1-shard table — co-location fast path applies. The Join runs at
        // the shard, and root demand (locality-agnostic SINGLETON) is satisfied directly.
        PlannerContext context = perIndexContext(Map.of("test_index", 1));
        RelNode join = buildEquiJoin("test_index", "test_index", JoinRelType.INNER);
        RelNode result = runPlanner(join, context);
        assertPlanShape("""
            OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testInnerJoin_differentTables_1shard() {
        // Different tables, even at 1 shard each — co-location predicate fails (different
        // tableIds). General path: ER per side, Join at coord.
        PlannerContext context = perIndexContext(Map.of("left_idx", 1, "right_idx", 1));
        RelNode join = buildEquiJoin("left_idx", "right_idx", JoinRelType.INNER);
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testInnerJoin_2shard() {
        PlannerContext context = perIndexContext(Map.of("left_idx", 2, "right_idx", 2));
        RelNode join = buildEquiJoin("left_idx", "right_idx", JoinRelType.INNER);
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testInnerJoin_mixedShards() {
        // left 1-shard, right 2-shard. Even if tableIds matched (which they don't here)
        // the predicate requires shardCount=1 on both sides — general path.
        PlannerContext context = perIndexContext(Map.of("left_idx", 1, "right_idx", 2));
        RelNode join = buildEquiJoin("left_idx", "right_idx", JoinRelType.INNER);
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testInnerJoin_mixedShards_leftMulti_rightSingle() {
        // Mirror of testInnerJoin_mixedShards.
        PlannerContext context = perIndexContext(Map.of("left_idx", 2, "right_idx", 1));
        RelNode join = buildEquiJoin("left_idx", "right_idx", JoinRelType.INNER);
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testLeftJoin_2shard() {
        runJoinKindShape(JoinRelType.LEFT, "left");
    }

    public void testRightJoin_2shard() {
        runJoinKindShape(JoinRelType.RIGHT, "right");
    }

    public void testFullJoin_2shard() {
        runJoinKindShape(JoinRelType.FULL, "full");
    }

    public void testCrossJoin_2shard() {
        // ON 1 = 1 — Calcite folds to TRUE: empty leftKeys, empty nonEqui, JoinInfo.isEqui true.
        PlannerContext context = perIndexContext(Map.of("left_idx", 2, "right_idx", 2));
        RelOptTable left = mockTable("left_idx", "status", "size");
        RelOptTable right = mockTable("right_idx", "status", "size");
        RexNode trueCondition = rexBuilder.makeLiteral(true);
        RelNode join = LogicalJoin.create(
            stubScan(left),
            stubScan(right),
            List.of(),
            trueCondition,
            Set.<CorrelationId>of(),
            JoinRelType.INNER
        );
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[true], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Join over a narrowing projection: the query reads only {@code status} from each side
     * of a [status, size] index. Field trimming inserts a narrowing Project per side, and the
     * post-CBO project-into-scan fold collapses each into a scan that declares only [status].
     * Unlike the aggregate case (where the split pushes the exchange above the narrowing
     * Project), the join gathers each scan input directly — so without the fold the shard would
     * still ship [status, size]. The folded scan narrows the native read in the shard fragment.
     */
    public void testInnerJoin_narrowingProjectFoldsIntoScan_2shard() {
        PlannerContext context = perIndexContext(Map.of("left_idx", 2, "right_idx", 2));
        RelOptTable left = mockTable("left_idx", "status", "size");
        RelOptTable right = mockTable("right_idx", "status", "size");
        // Project only the join keys (status from each side) so size is unused and trimmed.
        RelNode join = buildEquiJoin("left_idx", "right_idx", JoinRelType.INNER);
        RexNode leftKey = rexBuilder.makeInputRef(join, 0);
        RexNode rightKey = rexBuilder.makeInputRef(join, 2);
        RelNode plan = org.apache.calcite.rel.logical.LogicalProject.create(
            join,
            List.of(),
            List.of(leftKey, rightKey),
            List.of("ls", "rs")
        );
        RelNode result = runPlanner(plan, context);
        // Each scan's fields=[[status]] term (rendered only when the scan is narrowed) shows the fold
        // reached the scan through the per-side ER — the shard ships one column, not [status, size].
        assertPlanShape(
            """
                OpenSearchProject(ls=[$0], rs=[$1], viableBackends=[[mock-parquet]])
                  OpenSearchJoin(condition=[=($0, $1)], joinType=[inner], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[left_idx]], fields=[[status]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[right_idx]], fields=[[status]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Self-join with a narrowing projection: both sides scan the <em>same</em> index. The fold must
     * NOT narrow either scan — DataFusion registers one provider per index name, so the two scans
     * share a single base schema and narrowing one would break the other's column references (this
     * is the {@code appendcol}/self-join regression). The census-guard leaves both scans full
     * (no {@code fields=} term); pruning still happens via the narrowing Project the planner keeps.
     */
    public void testSelfJoin_narrowingProjectDoesNotFoldIntoScan_2shard() {
        PlannerContext context = perIndexContext(Map.of("test_index", 2));
        RelNode join = buildEquiJoin("test_index", "test_index", JoinRelType.INNER);
        RexNode leftKey = rexBuilder.makeInputRef(join, 0);
        RexNode rightKey = rexBuilder.makeInputRef(join, 2);
        RelNode plan = org.apache.calcite.rel.logical.LogicalProject.create(
            join,
            List.of(),
            List.of(leftKey, rightKey),
            List.of("ls", "rs")
        );
        RelNode result = runPlanner(plan, context);
        // No fields=[...] on either scan — both stay full because test_index is scanned twice.
        for (org.opensearch.analytics.planner.rel.OpenSearchTableScan scan : findScans(result)) {
            assertEquals("self-join scan must stay full (2 cols)", 2, scan.getRowType().getFieldCount());
        }
    }

    private static List<org.opensearch.analytics.planner.rel.OpenSearchTableScan> findScans(RelNode root) {
        List<org.opensearch.analytics.planner.rel.OpenSearchTableScan> scans = new java.util.ArrayList<>();
        if (root instanceof org.opensearch.analytics.planner.rel.OpenSearchTableScan ts) {
            scans.add(ts);
        }
        for (RelNode child : root.getInputs()) {
            scans.addAll(findScans(child));
        }
        return scans;
    }

    private void runJoinKindShape(JoinRelType joinType, String label) {
        PlannerContext context = perIndexContext(Map.of("left_idx", 2, "right_idx", 2));
        RelNode join = buildEquiJoin("left_idx", "right_idx", joinType);
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[__JOIN_TYPE__], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """
                .replace("__JOIN_TYPE__", label),
            result
        );
    }

    private RelNode buildEquiJoin(String leftTable, String rightTable, JoinRelType joinType) {
        RelOptTable left = mockTable(leftTable, "status", "size");
        RelOptTable right = mockTable(rightTable, "status", "size");
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(stubScan(left), stubScan(right), List.of(), cond, Set.<CorrelationId>of(), joinType);
    }
}
