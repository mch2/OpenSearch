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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end plan-shape assertions on the After-CBO RelNode produced by
 * {@link PlannerImpl#markAndOptimize}. These cases construct PPL-shaped
 * Calcite trees (skipping the PPL frontend) and assert the planner emits
 * a plan whose structure won't trip downstream backends — DataFusion
 * specifically — when the substrait is sent to them.
 */
public class PlanShapeTests extends BasePlannerRulesTests {

    private PlannerContext multiShardContext() {
        return buildContext("parquet", 3, intFields());
    }

    /**
     * PPL: {@code | stats count() as cnt by k | sort cnt | head 2 | fields k, cnt}
     *
     * <p>The PPL frontend emits a redundant outer Sort (no fetch, same collation as the inner)
     * plus an inner Sort with fetch above a column-swap Project above the Aggregate. With both
     * Sorts present, DataFusion's logical-plan optimizer eliminates the inner Sort as redundant
     * but keeps the Limit, then physical planning pushes the Limit BELOW the SortExec into a
     * {@code CoalescePartitionsExec(fetch=N)} on the FINAL Aggregate output. Result: fetch is
     * applied to the unsorted Aggregate output, and Sort runs on the wrong N rows.
     *
     * <p>{@link org.opensearch.analytics.planner.rules.OpenSearchSortRule} drops the redundant
     * outer Sort during HEP marking. After-CBO must contain at most one Sort over the FINAL
     * Aggregate, with that Sort carrying the fetch.
     */
    public void testSortHeadAfterStats_dropsRedundantOuterSort() {
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ true);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Without the redundant outer Sort the planner must NOT drop the inner Sort+fetch — the
     * fetch is the only thing preserving top-K semantics.
     */
    public void testSortHeadAfterStats_singleSortFetchPreserved() {
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ false);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * If the outer Sort sorts by a DIFFERENT key than the inner Sort+fetch, the outer is NOT
     * redundant — dropping it would change result ordering. The rule's collation comparison
     * (after remapping through the Project) must reject the drop.
     */
    public void testSortHeadAfterStats_outerSortWithDifferentKeyKept() {
        // Inner sort by cnt ($0 below swap), outer sort by k ($0 above swap which maps to k).
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ true, /* outerSortField */ 0);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchProject(k=[$1], cnt=[$0], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[2], viableBackends=[[mock-parquet]])
                      OpenSearchProject(cnt=[$1], k=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                          OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                            OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Multi-shard {@code stats by k} must split into PARTIAL+ExchangeReducer+FINAL. The
     * ExchangeReducer must be SINGLETON (single coordinator-side gather) and sit between
     * the two Aggregate phases.
     */
    public void testStatsByKey_multiShardSplits() {
        AggregateCall countCall = countStarCall();
        RelNode input = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countCall);
        RelNode result = runPlanner(input, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Two multi-shard scans joined on an equi-condition. The planner must wrap each join
     * input in exactly one {@link OpenSearchExchangeReducer} so both sides gather to the
     * coordinator before the hash join runs. The join itself is stamped SINGLETON by
     * {@link org.opensearch.analytics.planner.rules.OpenSearchJoinRule} wraps each input
     * in an ER at HEP marking time, so no extra top-level gather is needed.
     *
     * <pre>
     * OpenSearchJoin(INNER, $0 = $2)           ← SINGLETON
     *   ├── OpenSearchExchangeReducer
     *   │     └── OpenSearchTableScan(test_index)
     *   └── OpenSearchExchangeReducer
     *         └── OpenSearchTableScan(test_index)
     * </pre>
     */
    public void testJoinWithoutAggregate_erPerSideDirectlyAboveScans() {
        PlannerContext context = multiShardContext();
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode left = stubScan(table);
        RelNode right = stubScan(table);

        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(left, right, List.of(), condition, Set.of(), JoinRelType.INNER);

        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Each side of the join pre-aggregates with {@code count()} before the join. The planner
     * splits each Aggregate into PARTIAL + FINAL with a gather between them. The join's
     * {@code convert(input, SINGLETON)} is a no-op because each FINAL already delivers
     * SINGLETON — no extra ER wraps the FINAL on either side.
     *
     * <pre>
     * OpenSearchJoin(INNER, $0 = $2)           ← stamped SINGLETON by JoinSplitRule
     *   ├── OpenSearchAggregate(FINAL)
     *   │     └── OpenSearchExchangeReducer
     *   │           └── OpenSearchAggregate(PARTIAL)
     *   │                 └── OpenSearchTableScan(test_index)
     *   └── OpenSearchAggregate(FINAL)
     *         └── OpenSearchExchangeReducer
     *               └── OpenSearchAggregate(PARTIAL)
     *                     └── OpenSearchTableScan(test_index)
     * </pre>
     *
     * <p>Exactly two ERs — one between PARTIAL and FINAL on each branch. No top-level
     * redundant ER: the join delivers SINGLETON directly.
     */
    public void testJoinWithAggregate_erBetweenPartialAndFinal_noExtraErAboveFinal() {
        PlannerContext context = multiShardContext();

        RelNode leftAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode rightAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());

        // Equi-join on grouping key ($0 on each side). Left rowType is (status, cnt); right is
        // (status, cnt). After join the output has 4 columns; condition references left.$0 and
        // right.$0 (offset by left fieldCount=2).
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(leftAgg, rightAgg, List.of(), condition, Set.of(), JoinRelType.INNER);

        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Asserts Project(COALESCE) → Aggregate(FINAL) → ER → Aggregate(PARTIAL) → TableScan
     * for a COUNT-containing branch. The Project wrap exists because SUM(partial_count)
     * returns nullable BIGINT and the original COUNT is non-nullable; the wrap
     * re-establishes the original row type.
     */
    private static void assertCountAggregateChain(RelNode branchRoot) {
        assertTrue(
            "branch root must be OpenSearchProject (COALESCE wrap), got " + branchRoot.getClass().getSimpleName(),
            branchRoot instanceof OpenSearchProject
        );
        RelNode beneathProject = RelNodeUtils.unwrapHep(((OpenSearchProject) branchRoot).getInput());
        assertTrue(
            "Project wrap's child must be OpenSearchAggregate(FINAL), got " + beneathProject.getClass().getSimpleName(),
            beneathProject instanceof OpenSearchAggregate
        );
        OpenSearchAggregate finalAgg = (OpenSearchAggregate) beneathProject;
        assertEquals("wrapped aggregate must be FINAL", AggregateMode.FINAL, finalAgg.getMode());

        RelNode beneathFinal = RelNodeUtils.unwrapHep(finalAgg.getInput());
        assertTrue(
            "FINAL aggregate's child must be OpenSearchExchangeReducer, got " + beneathFinal.getClass().getSimpleName(),
            beneathFinal instanceof OpenSearchExchangeReducer
        );
        RelNode beneathEr = RelNodeUtils.unwrapHep(((OpenSearchExchangeReducer) beneathFinal).getInput());
        assertTrue(
            "ER's child must be OpenSearchAggregate(PARTIAL), got " + beneathEr.getClass().getSimpleName(),
            beneathEr instanceof OpenSearchAggregate
        );
        assertEquals("beneath-ER aggregate must be PARTIAL", AggregateMode.PARTIAL, ((OpenSearchAggregate) beneathEr).getMode());
        RelNode beneathPartial = RelNodeUtils.unwrapHep(beneathEr.getInputs().get(0));
        assertTrue(
            "PARTIAL aggregate's child must be OpenSearchTableScan, got " + beneathPartial.getClass().getSimpleName(),
            beneathPartial instanceof OpenSearchTableScan
        );
    }

    private PlannerContext singleShardContext() {
        return buildContext("parquet", 1, intFields());
    }

    // ── builders ───────────────────────────────────────────────────────────────

    private RelNode topKAfterStats(boolean withRedundantOuterSort) {
        return topKAfterStats(withRedundantOuterSort, /* outerSortField */ 1);
    }

    /**
     * Builds the Calcite tree the PPL frontend emits for
     * {@code | stats count() as cnt by k | sort cnt | head 2 | fields k, cnt}:
     *
     * <pre>
     * LogicalSort(sort0=$outerSortField)?              -- outer "sort cnt", optional
     *   LogicalProject(k=$1, cnt=$0)                   -- "fields k, cnt" (swap)
     *     LogicalSort(sort0=$0, fetch=2)               -- "sort cnt | head 2"
     *       LogicalAggregate(group=[{0}], cnt=COUNT()) -- "stats count() by k"
     *         StubTableScan
     * </pre>
     */
    private RelNode topKAfterStats(boolean withRedundantOuterSort, int outerSortField) {
        // Aggregate: group by column 0 (k), count as column 1 (cnt). Output: (k, cnt).
        AggregateCall countCall = countStarCall();
        RelNode agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countCall);

        // Inner Sort+fetch: sort by cnt ($1 in agg's output: column 1).
        // Wait — agg's output is (k=$0, cnt=$1). We want to sort by cnt, so sort field = 1.
        // But the BasePlannerRulesTests' makeSort hardcodes field 0. Use a custom builder.
        RelNode innerSort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        // Project that swaps to (k=$1 from cnt-after-sort wait no — keep original schema).
        // Match PPL output: (cnt=cnt, k=k) reorder. Actually PPL's "fields k, cnt" produces output
        // (k, cnt). Inner sort's output is still (k=$0, cnt=$1) because Sort doesn't change schema.
        // After Project (k=$0, cnt=$1) — identity. But the real PPL plan has a SWAP — the inner sort
        // in real PPL sees (cnt=$0, k=$1) due to a prior swap. Mimic that by adding swaps both sides.
        //
        // Simpler: just use the swap that mirrors the After-CBO plan we observed:
        // Project(k=$1, cnt=$0) over an input whose output is (cnt=$0, k=$1).
        // To produce that, add a swap project BELOW the inner sort too.
        RelNode innerSwap = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "k")
        );
        RelNode innerSortOverSwap = LogicalSort.create(
            innerSwap,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode outerSwap = LogicalProject.create(
            innerSortOverSwap,
            List.of(),
            List.of(rexBuilder.makeInputRef(innerSortOverSwap, 1), rexBuilder.makeInputRef(innerSortOverSwap, 0)),
            List.of("k", "cnt")
        );

        if (!withRedundantOuterSort) {
            return outerSwap;
        }

        // Outer Sort: collation field = `outerSortField` ($1 = cnt for redundant, $0 = k for non-redundant).
        return LogicalSort.create(
            outerSwap,
            RelCollations.of(new RelFieldCollation(outerSortField, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
    }

    // ── IT-shape coverage: aggregate × join × sort compositions ──────────────
    //
    // Each shape has single-shard and multi-shard variants.
    //
    // Single-shard scans declare SOURCE(SINGLETON) and satisfy the root's
    // RESULT(SINGLETON) demand directly — no ER above. Multi-shard scans declare
    // SOURCE(RANDOM); split rules drive ER insertion below operators that require
    // SINGLETON input (Aggregate→FINAL, collated Sort, Join inputs).

    /**
     * PPL: {@code source=t | head 10} (pure scan with LIMIT, no stats).
     * Pure LIMIT Sort (no collation) is exempt from the SINGLETON requirement (see
     * {@link OpenSearchSort#computeSelfCost}). The Sort sits above a partition-local scan
     * and the ER appears above the Sort, not below it (fetch-then-gather).
     * Multi-shard: one ER above the Sort. Single-shard: no ERs.
     */
    public void testLimitAfterScan_multiShard_noForceSingleton() {
        RelNode plan = buildLimitAfterScan();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testLimitAfterScan_singleShard_noTopER() {
        RelNode plan = buildLimitAfterScan();
        RelNode result = runPlanner(plan, singleShardContext());
        // Single-shard: scan is SOURCE(SINGLETON), which satisfies root's RESULT(SINGLETON)
        // demand without a gather — plan collapses to a single stage, no top ER.
        assertPlanShape("""
            OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * PPL: {@code source=t}
     * Baseline: a bare scan at root. Volcano's root SINGLETON request wraps the RANDOM scan in one ER.
     */
    public void testPureScan_multiShard_rootER() {
        RelNode plan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testPureScan_singleShard_noTopER() {
        RelNode plan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(plan, singleShardContext());
        // Single-shard bare scan: SOURCE(SINGLETON) already satisfies root's RESULT(SINGLETON)
        // demand, so no ER is inserted — plan is just the scan.
        assertPlanShape("""
            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    private void assertPureScanShape(RelNode result) {
        assertTrue(
            "root should be OpenSearchExchangeReducer wrapping the scan, got " + result.getClass().getSimpleName(),
            result instanceof OpenSearchExchangeReducer
        );
        RelNode beneath = RelNodeUtils.unwrapHep(((OpenSearchExchangeReducer) result).getInput());
        assertTrue(
            "ER's input must be OpenSearchTableScan, got " + beneath.getClass().getSimpleName(),
            beneath instanceof OpenSearchTableScan
        );
        assertEquals("exactly one ER", 1, collectAll(result, OpenSearchExchangeReducer.class).size());
    }

    /**
     * PPL: {@code stats count() by status | inner join ... [source=u | stats count() by size]}
     * Two aggregates with different group keys, joined on some key. Both FINALs deliver SINGLETON;
     * JoinSplit's convert() is a no-op per side; no extra ER above either FINAL. Total ERs: 2
     * (one per branch's PARTIAL→FINAL).
     */
    public void testJoinWithDifferentGroupKeys_multiShard() {
        RelNode plan = buildJoinWithDifferentGroupKeys();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testJoinWithDifferentGroupKeys_singleShard() {
        RelNode plan = buildJoinWithDifferentGroupKeys();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], s=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{1}], s=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $0)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    private void assertJoinWithDifferentGroupKeysShape(RelNode result) {
        OpenSearchJoin join = findFirst(result, OpenSearchJoin.class);
        assertNotNull(join);
        List<OpenSearchAggregate> aggs = collectAll(result, OpenSearchAggregate.class);
        long finalCount = aggs.stream().filter(a -> a.getMode() == AggregateMode.FINAL).count();
        assertEquals("two FINAL aggregates (one per branch)", 2, finalCount);
        List<OpenSearchExchangeReducer> ers = collectAll(result, OpenSearchExchangeReducer.class);
        assertEquals("two ERs (one per branch's PARTIAL→FINAL)", 2, ers.size());
    }

    // ── Mixed-shard join variants: one side single-shard, the other multi-shard ───
    //
    // A single-shard scan is SINGLETON (already at one node). A multi-shard scan is
    // RANDOM. When joined together, JoinSplitRule's per-side convert() inserts an ER
    // only on the RANDOM side. The SINGLETON side passes through untouched.

    /** Left scan single-shard, right scan multi-shard. One ER, above right scan. */
    public void testJoinMixedShards_leftSingle_rightMulti() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("left_idx", 1, "right_idx", 3));
        RelNode join = buildJoinOfTwoScans("left_idx", "right_idx");
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Left scan multi-shard, right scan single-shard. Mirror. One ER, above left scan. */
    public void testJoinMixedShards_leftMulti_rightSingle() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("left_idx", 3, "right_idx", 1));
        RelNode join = buildJoinOfTwoScans("left_idx", "right_idx");
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Both scans single-shard. Join has SINGLETON inputs already; no ER anywhere. */
    public void testJoinMixedShards_bothSingle() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("left_idx", 1, "right_idx", 1));
        RelNode join = buildJoinOfTwoScans("left_idx", "right_idx");
        RelNode result = runPlanner(join, context);
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]], strategy=[CoordinatorHashJoin])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Union coverage: multisearch / appendpipe shapes ──────────────────────
    //
    // Three structural cases × two shard variants. Every arm gets wrapped in an ER at HEP
    // marking time (OpenSearchUnionRule) so DAGBuilder can cut a separate stage per branch
    // — this is how per-branch ShardTargetResolver routing works for arms that may scan
    // different indices.

    /** Two multi-shard scans, same index, unioned. Each arm gets its own ER over the scan. */
    public void testUnion_twoArmScans_multiShard() {
        RelNode union = buildUnionOfTwoScans("test_index", "test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 3));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_twoArmScans_singleShard() {
        RelNode union = buildUnionOfTwoScans("test_index", "test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        // Each arm gets a HEP-time ER even over a single-shard scan — the ER is the
        // stage-cut marker DAGBuilder needs to split per-arm stages. ConverterImpl dedup
        // only fires when origins match (e.g. ER over a FINAL aggregate at GATHERED);
        // ER(GATHERED) over Scan(SCAN) lives in a distinct subset and survives.
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Each arm has stats — arm-level aggregate split produces FINAL at EXECUTION(SINGLETON).
     * The Union-arm ER that OpenSearchUnionRule wrapped over the marked input gets deduped
     * by Volcano because FINAL already delivers EXECUTION(SINGLETON) — the ConverterImpl
     * ER lands in the same RelSet as the FINAL and is redundant. Each Union input is the
     * FINAL directly.
     */
    public void testUnion_twoArmsWithStats_multiShard() {
        RelNode union = buildUnionOfTwoStatsArms("test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 3));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_twoArmsWithStats_singleShard() {
        RelNode union = buildUnionOfTwoStatsArms("test_index");
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Each arm scans a different index — exercises the per-branch stage isolation requirement. */
    public void testUnion_twoArmsDifferentIndices_multiShard() {
        RelNode union = buildUnionOfTwoScans("left_idx", "right_idx");
        RelNode result = runPlanner(union, unionContextTwoIndices(Map.of("left_idx", 3, "right_idx", 3)));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_twoArmsDifferentIndices_singleShard() {
        RelNode union = buildUnionOfTwoScans("left_idx", "right_idx");
        RelNode result = runPlanner(union, unionContextTwoIndices(Map.of("left_idx", 1, "right_idx", 1)));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Union builders / contexts ─────────────────────────────────────────────

    private RelNode buildUnionOfTwoScans(String leftTable, String rightTable) {
        RelNode left = stubScan(mockTable(leftTable, "status", "size"));
        RelNode right = stubScan(mockTable(rightTable, "status", "size"));
        return LogicalUnion.create(List.of(left, right), /* all */ true);
    }

    private RelNode buildUnionOfTwoStatsArms(String table) {
        RelNode arm1 = org.apache.calcite.rel.logical.LogicalAggregate.create(
            stubScan(mockTable(table, "status", "size")),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        RelNode arm2 = org.apache.calcite.rel.logical.LogicalAggregate.create(
            stubScan(mockTable(table, "status", "size")),
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        return LogicalUnion.create(List.of(arm1, arm2), /* all */ true);
    }

    /** Planner context with UNION engine capability declared, for a single index. */
    private PlannerContext unionContextSingleIndex(String indexName, int shardCount) {
        return buildContextPerIndex("parquet", Map.of(indexName, shardCount), intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /** Planner context with UNION engine capability declared, across multiple indices. */
    private PlannerContext unionContextTwoIndices(Map<String, Integer> shardsByIndex) {
        return buildContextPerIndex("parquet", shardsByIndex, intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /** MockDataFusionBackend with EngineCapability.UNION declared. */
    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<org.opensearch.analytics.spi.EngineCapability> supportedEngineCapabilities() {
            Set<org.opensearch.analytics.spi.EngineCapability> caps = new java.util.HashSet<>(super.supportedEngineCapabilities());
            caps.add(org.opensearch.analytics.spi.EngineCapability.UNION);
            return caps;
        }
    }

    private RelNode buildJoinOfTwoScans(String leftTable, String rightTable) {
        RelOptTable left = mockTable(leftTable, "status", "size");
        RelOptTable right = mockTable(rightTable, "status", "size");
        RelNode leftScan = stubScan(left);
        RelNode rightScan = stubScan(right);
        // equi-join on status column — left.$0 == right.$0 (offset by left fieldCount=2)
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(leftScan, rightScan, List.of(), cond, Set.of(), JoinRelType.INNER);
    }

    // ── Logical-plan builders for the IT-shape tests ──────────────────────────

    private RelNode buildLimitAfterScan() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        // head 10: pure LIMIT, empty collation
        return LogicalSort.create(
            scan,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    private RelNode buildJoinWithDifferentGroupKeys() {
        RelOptTable table = mockTable("test_index", "status", "size");
        // Left: stats sum(size) by status (group=0, aggregation output INTEGER)
        RelNode leftScan = stubScan(table);
        RelNode leftAgg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            leftScan,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(0),
            null,
            List.of(sumCallOn(leftScan, /* sumField */ 1))
        );
        // Right: stats sum(status) by size (group=1, aggregation output INTEGER — different group key)
        RelNode rightScan = stubScan(table);
        RelNode rightAgg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            rightScan,
            List.of(),
            org.apache.calcite.util.ImmutableBitSet.of(1),
            null,
            List.of(sumCallOn(rightScan, /* sumField */ 0))
        );
        // Join on left.$0 (status, INTEGER) == right.$0 (size, INTEGER).
        // Arbitrary equi-condition; we only care about plan shape, not join semantics.
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(leftAgg, rightAgg, List.of(), cond, Set.of(), JoinRelType.INNER);
    }

    private AggregateCall sumCallOn(RelNode input, int sumField) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(sumField),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "s"
        );
    }

    private AggregateCall countStarCallOn(RelNode input) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
    }

    // ── plan walkers ───────────────────────────────────────────────────────────

    private List<OpenSearchSort> collectSorts(RelNode node) {
        java.util.ArrayList<OpenSearchSort> out = new java.util.ArrayList<>();
        collectInto(node, OpenSearchSort.class, out);
        return out;
    }

    private static <T extends RelNode> List<T> collectAll(RelNode node, Class<T> type) {
        java.util.ArrayList<T> out = new java.util.ArrayList<>();
        collectInto(node, type, out);
        return out;
    }

    private static <T extends RelNode> void collectInto(RelNode node, Class<T> type, java.util.List<T> out) {
        if (type.isInstance(node)) out.add(type.cast(node));
        for (RelNode child : node.getInputs())
            collectInto(child, type, out);
    }

    private static <T extends RelNode> T findFirst(RelNode root, Class<T> type) {
        if (type.isInstance(root)) return type.cast(root);
        for (RelNode child : root.getInputs()) {
            T found = findFirst(child, type);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Asserts {@link RelOptUtil#toString(RelNode)} equals the expected text-block. On mismatch
     * the full actual plan is shown so reviewers see the complete tree.
     */
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
            while (end > 0 && (line.charAt(end - 1) == ' ' || line.charAt(end - 1) == '\t'))
                end--;
            sb.append(line, 0, end).append('\n');
        }
        while (sb.length() >= 2 && sb.charAt(sb.length() - 1) == '\n' && sb.charAt(sb.length() - 2) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
}
