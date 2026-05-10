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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.analytics.spi.WindowFunctionCapability;

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
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        // Top of the tree must be the swap Project, NOT a redundant outer Sort.
        assertTrue(
            "redundant outer Sort must be dropped; root was " + result.getClass().getSimpleName(),
            result instanceof OpenSearchProject
        );
        // Exactly one OpenSearchSort in the chain, with fetch set.
        List<OpenSearchSort> sorts = collectSorts(result);
        assertEquals("expected exactly one OpenSearchSort after redundant-outer drop", 1, sorts.size());
        assertNotNull("the surviving Sort must carry the fetch", sorts.get(0).fetch);
    }

    /**
     * Without the redundant outer Sort the planner must NOT drop the inner Sort+fetch — the
     * fetch is the only thing preserving top-K semantics.
     */
    public void testSortHeadAfterStats_singleSortFetchPreserved() {
        RelNode input = topKAfterStats(/* withRedundantOuterSort */ false);
        RelNode result = runPlanner(input, multiShardContext());
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        List<OpenSearchSort> sorts = collectSorts(result);
        assertEquals("standalone Sort+fetch must survive", 1, sorts.size());
        assertNotNull(sorts.get(0).fetch);
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
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        List<OpenSearchSort> sorts = collectSorts(result);
        assertEquals("outer Sort with different key must be kept", 2, sorts.size());
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
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        // Walk down: FINAL Aggregate → Project (the COALESCE wrap when the agg is a COUNT) or
        // straight to ExchangeReducer; in either case a Sort+fetch consumer above won't exist
        // here, so the gather must be the very next OpenSearchExchangeReducer below the FINAL.
        OpenSearchAggregate finalAgg = findFirst(result, OpenSearchAggregate.class);
        assertNotNull("expected an OpenSearchAggregate in the plan", finalAgg);
        assertEquals("top aggregate must be FINAL after multi-shard split", AggregateMode.FINAL, finalAgg.getMode());
        OpenSearchExchangeReducer gather = findFirst(finalAgg, OpenSearchExchangeReducer.class);
        assertNotNull("FINAL aggregate must have an ExchangeReducer below it", gather);
        OpenSearchAggregate partialAgg = findFirst(gather, OpenSearchAggregate.class);
        assertNotNull("ExchangeReducer must have a PARTIAL aggregate below", partialAgg);
        assertEquals(AggregateMode.PARTIAL, partialAgg.getMode());
    }

    /**
     * PPL: {@code source=t | sort name | streamstats count() as running | eventstats max(running) as mx | head 1 | fields mx}
     *
     * <p>Two stacked windowed Projects with non-windowed ancestors (top {@code Sort fetch=1} and
     * top {@code Project mx}) above them. Each windowed Project requires SINGLETON input — the
     * windowed gather rule wraps each with an {@code ExchangeReducer}. Then the SINGLETON
     * requirement must propagate up through the non-windowed Sort and Project ancestors so the
     * root SINGLETON request has a finite-cost path.
     *
     * <p>Without per-op distribution-derive rules for Sort/Project (which propagate child
     * SINGLETON traits to a parent variant), Volcano can't construct that path and CBO throws
     * {@code CannotPlanException}.
     */
    public void testStackedWindowed_topSortAncestor_compiles() {
        RelNode input = stackedWindowedTopK();
        RelNode result = runPlanner(input, multiShardWindowContext());
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        assertNotNull("planner returned a plan", result);
        OpenSearchExchangeReducer er = findFirst(result, OpenSearchExchangeReducer.class);
        assertNotNull("expected at least one OpenSearchExchangeReducer in the plan", er);
    }

    /**
     * PPL: {@code source=t | eventstats max(value) as mx | where mx > 5 | head 1 | fields mx}
     *
     * <p>Filter sits between the windowed Project (gathered to SINGLETON by the windowed
     * gather rule) and the root. The Filter distribution-derive rule must propagate
     * SINGLETON up through the Filter so the root has a finite-cost path.
     */
    public void testWindowedThenWhere_compiles() {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        RelNode scan = stubScan(table);

        // eventstats max(value) as mx → Project(name, value, mx=MAX(value) OVER ())
        LogicalProject eventstats = LogicalProject.create(
            scan,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(scan, 0),
                rexBuilder.makeInputRef(scan, 1),
                makeGlobalMaxOver(scan, 1)
            ),
            List.of("name", "value", "mx")
        );

        // where mx > 5 → Filter on column 2 ($2 > 5)
        RexNode mxRef = rexBuilder.makeInputRef(eventstats, 2);
        RexNode five = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, mxRef, five);
        org.apache.calcite.rel.logical.LogicalFilter filter = org.apache.calcite.rel.logical.LogicalFilter.create(eventstats, condition);

        // head 1: Sort fetch=1
        RelNode topSort = LogicalSort.create(
            filter,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        // fields mx
        RelNode top = LogicalProject.create(
            topSort,
            List.of(),
            List.of(rexBuilder.makeInputRef(topSort, 2)),
            List.of("mx")
        );

        RelNode result = runPlanner(top, multiShardWindowContext());
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertNotNull("planner returned a plan", result);
        OpenSearchExchangeReducer er = findFirst(result, OpenSearchExchangeReducer.class);
        assertNotNull("expected at least one OpenSearchExchangeReducer in the plan", er);
    }

    private PlannerContext multiShardWindowContext() {
        MockDataFusionBackend dfWithWindowFns = new MockDataFusionBackend() {
            @Override
            protected Set<WindowFunctionCapability> windowFunctionCapabilities() {
                String parquet = MockDataFusionBackend.PARQUET_DATA_FORMAT;
                return Set.of(
                    new WindowFunctionCapability(WindowFunction.COUNT, Set.of(FieldType.INTEGER, FieldType.LONG, FieldType.KEYWORD), Set.of(parquet)),
                    new WindowFunctionCapability(WindowFunction.MAX, Set.of(FieldType.INTEGER, FieldType.LONG), Set.of(parquet))
                );
            }
        };
        return buildContext(
            "parquet",
            3,
            Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "integer")),
            List.of(dfWithWindowFns, LUCENE)
        );
    }

    /** Empty {@code OVER ()} — global aggregate broadcast (eventstats). */
    private RexNode makeGlobalMaxOver(RelNode input, int operandIdx) {
        return rexBuilder.makeOver(
            input.getRowType().getFieldList().get(operandIdx).getType(),
            SqlStdOperatorTable.MAX,
            List.of(rexBuilder.makeInputRef(input, operandIdx)),
            List.of(),
            ImmutableList.<RexFieldCollation>of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
    }

    /** Running count: {@code COUNT() OVER (ROWS UNBOUNDED PRECEDING)} (streamstats count()). */
    private RexNode makeRunningCountOver() {
        return rexBuilder.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            SqlStdOperatorTable.COUNT,
            List.of(),
            List.of(),
            ImmutableList.<RexFieldCollation>of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false,
            false
        );
    }

    /**
     * Builds the Calcite tree the PPL frontend emits for
     * {@code source=t | sort name | streamstats count() as running | eventstats max(running) as mx | head 1 | fields mx}:
     *
     * <pre>
     * LogicalProject(mx=$3)                                          -- "fields mx"
     *   LogicalSort(fetch=1)                                          -- "head 1"
     *     LogicalProject(name=$0, value=$1, running=$2,
     *                    mx=MAX($2) OVER ())                          -- eventstats
     *       LogicalProject(name=$0, value=$1,
     *                      running=COUNT() OVER (ROWS UNBOUNDED PRECEDING))  -- streamstats
     *         LogicalSort(name asc)                                   -- inner "sort name"
     *           LogicalTableScan(name, value)                         -- multi-shard
     * </pre>
     */
    private RelNode stackedWindowedTopK() {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        RelNode scan = stubScan(table);

        // Inner sort by name (column 0) ascending.
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );

        // streamstats: project (name, value, running=COUNT() OVER (ROWS UNBOUNDED PRECEDING))
        LogicalProject streamstats = LogicalProject.create(
            innerSort,
            List.of(),
            List.of(rexBuilder.makeInputRef(innerSort, 0), rexBuilder.makeInputRef(innerSort, 1), makeRunningCountOver()),
            List.of("name", "value", "running")
        );

        // eventstats: project (name, value, running, mx=MAX(running) OVER ())
        LogicalProject eventstats = LogicalProject.create(
            streamstats,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(streamstats, 0),
                rexBuilder.makeInputRef(streamstats, 1),
                rexBuilder.makeInputRef(streamstats, 2),
                makeGlobalMaxOver(streamstats, 2)
            ),
            List.of("name", "value", "running", "mx")
        );

        // head 1: Sort fetch=1 with empty collation.
        RelNode topSort = LogicalSort.create(
            eventstats,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        // fields mx: project just $3.
        return LogicalProject.create(
            topSort,
            List.of(),
            List.of(rexBuilder.makeInputRef(topSort, 3)),
            List.of("mx")
        );
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
        //   Project(k=$1, cnt=$0) over an input whose output is (cnt=$0, k=$1).
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

    // ── plan walkers ───────────────────────────────────────────────────────────

    private List<OpenSearchSort> collectSorts(RelNode node) {
        java.util.ArrayList<OpenSearchSort> out = new java.util.ArrayList<>();
        collectInto(node, OpenSearchSort.class, out);
        return out;
    }

    private static <T extends RelNode> void collectInto(RelNode node, Class<T> type, java.util.List<T> out) {
        if (type.isInstance(node)) out.add(type.cast(node));
        for (RelNode child : node.getInputs()) collectInto(child, type, out);
    }

    private static <T extends RelNode> T findFirst(RelNode root, Class<T> type) {
        if (type.isInstance(root)) return type.cast(root);
        for (RelNode child : root.getInputs()) {
            T found = findFirst(child, type);
            if (found != null) return found;
        }
        return null;
    }
}
