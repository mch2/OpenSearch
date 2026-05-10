/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScanCapability;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link PlanForker} — verifies plan alternatives are generated correctly
 * for different query shapes with two viable backends.
 *
 * <p>All tests use duplicated doc values (both parquet and lucene) so both backends
 * are viable, verifying that forking produces two alternatives and each is narrowed
 * to exactly one backend.
 */
public class PlanForkerTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(PlanForkerTests.class);

    private static final Set<FieldType> SUPPORTED_TYPES = Set.of(
        FieldType.INTEGER,
        FieldType.LONG,
        FieldType.KEYWORD,
        FieldType.DATE,
        FieldType.BOOLEAN
    );

    private QueryDAG buildAndFork(int shardCount, RelNode logicalPlan) {
        MockLuceneBackend luceneWithScanAndAgg = new MockLuceneBackend() {
            @Override
            protected Set<ScanCapability> scanCapabilities() {
                return Set.of(new ScanCapability.DocValues(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), SUPPORTED_TYPES));
            }

            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(
                    Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER), AggregateFunction.COUNT, Set.of(FieldType.INTEGER))
                );
            }

            @Override
            protected Set<EngineCapability> supportedEngineCapabilities() {
                return Set.of(EngineCapability.SORT);
            }
        };
        var context = buildContextWithExplicitStorage(shardCount, duplicatedIntFields(), List.of(DATAFUSION, luceneWithScanAndAgg));
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after forking:\n{}", dag);
        return dag;
    }

    /**
     * Asserts a stage has exactly two alternatives (one per backend), each narrowed to a single backend.
     * TODO: extend with randomized tests that pick N backends from a pool and assert exactly N alternatives.
     * TODO: add delegation-aware forking tests once delegation is implemented — verify that when
     * annotation viable backends differ from operator backend, one plan per annotation target is generated
     * (e.g. DF operator with Lucene annotation for filter delegation produces a separate alternative).
     */
    /**
     * Walks the DAG depth-first and returns the first stage whose fragment root is
     * an instance of {@code expected}. Used to find the data-node stage (where the
     * test's operator lives) without coupling tests to the coord/data-node split —
     * after the always-RANDOM scan trait change, single-shard plans gain a coord
     * stage above the data-node fragment, so {@code dag.rootStage()} no longer
     * holds the operator the test wants to inspect.
     */
    private static Stage findStageWithFragment(QueryDAG dag, Class<? extends OpenSearchRelNode> expected) {
        Stage found = findStageWithFragmentInTree(dag.rootStage(), expected);
        if (found == null) {
            throw new AssertionError("No stage in DAG holds a " + expected.getSimpleName() + " fragment");
        }
        return found;
    }

    private static Stage findStageWithFragmentInTree(Stage stage, Class<? extends OpenSearchRelNode> expected) {
        // Post-order: prefer deepest match. The split aggregate produces a FINAL on the
        // coord stage and a PARTIAL on the data-node stage; both are OpenSearchAggregate,
        // and the test wants the data-node one (where forking produces N alternatives).
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageWithFragmentInTree(child, expected);
            if (found != null) return found;
        }
        if (expected.isInstance(stage.getFragment())) return stage;
        return null;
    }

    private static void assertTwoAlternatives(Stage stage, Class<? extends OpenSearchRelNode> expectedRootType) {
        List<StagePlan> alternatives = stage.getPlanAlternatives();
        assertEquals("expected two alternatives (one per viable backend)", 2, alternatives.size());
        for (StagePlan plan : alternatives) {
            assertNotNull(plan.resolvedFragment());
            assertTrue(
                "resolved fragment root must be " + expectedRootType.getSimpleName(),
                expectedRootType.isInstance(plan.resolvedFragment())
            );
            assertEquals(
                "viableBackends must be narrowed to single backend",
                1,
                ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().size()
            );
            assertEquals(plan.backendId(), ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().getFirst());
        }
        assertNotEquals("both alternatives must have distinct backends", alternatives.get(0).backendId(), alternatives.get(1).backendId());
    }

    /**
     * Single-shard scan, filter, and aggregate — the data-node stage (where the operator
     * lives) gets two alternatives, one per backend. Tests find the operator's stage
     * via fragment type rather than assuming the DAG is single-stage; the always-RANDOM
     * scan trait gives every plan a coord stage above the data-node fragment.
     */
    public void testSingleStageQueryShapes() {
        QueryDAG scanDag = buildAndFork(1, stubScan(mockTable("test_index", "status", "size")));
        assertTwoAlternatives(findStageWithFragment(scanDag, OpenSearchTableScan.class), OpenSearchTableScan.class);

        QueryDAG filterDag = buildAndFork(
            1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200))
        );
        assertTwoAlternatives(findStageWithFragment(filterDag, OpenSearchFilter.class), OpenSearchFilter.class);

        QueryDAG aggDag = buildAndFork(1, makeAggregate(sumCall()));
        assertTwoAlternatives(findStageWithFragment(aggDag, OpenSearchAggregate.class), OpenSearchAggregate.class);
    }

    /**
     * Sort(Filter(Scan)) and Sort(Agg(Filter(Scan))) with limit — verifies forking
     * produces two alternatives with correct pipeline shape at each level.
     */
    public void testSortQueryShapes() {
        // Sort(Filter(Scan)) with limit. Sort runs on SINGLETON (correctness-required —
        // a partition-local sort isn't a global sort), so Sort lives on the coord stage.
        // The coord stage gets one alternative per backend with an ExchangeSinkProvider
        // (only DF in the mock setup). The data-node stage holds the scan and gets
        // two alternatives (one per backend with a scan capability).
        QueryDAG sortFilterDag = buildAndFork(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10)
        );
        // Filter pushes down to the data-node side now (cost penalty on SINGLETON Filter
        // makes the RANDOM-Filter + ER-above plan cheaper), so the data-node stage's
        // root is the Filter, not the Scan. Forking still produces two alternatives.
        Stage filterStage = findStageWithFragment(sortFilterDag, OpenSearchFilter.class);
        assertTwoAlternatives(filterStage, OpenSearchFilter.class);

        // Sort(Agg(Filter(Scan))) with limit — Aggregate splits across coord/data-node,
        // Filter runs on the data-node side (with PARTIAL agg over Filter over Scan).
        QueryDAG sortAggDag = buildAndFork(
            1,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            )
        );
        Stage partialAggStage = findStageWithFragment(sortAggDag, OpenSearchAggregate.class);
        assertTwoAlternatives(partialAggStage, OpenSearchAggregate.class);
        for (StagePlan plan : partialAggStage.getPlanAlternatives()) {
            assertPipelineViableBackends(
                plan.resolvedFragment(),
                List.of(OpenSearchAggregate.class, OpenSearchFilter.class, OpenSearchTableScan.class),
                Set.of(plan.backendId())
            );
        }
    }

    /**
     * Aggregate(Filter(Scan)) — most common OLAP shape. Verifies that forking narrows
     * annotations consistently through the entire tree: both the aggregate root and the
     * filter child in each alternative must be narrowed to the same single backend.
     *
     * TODO: with delegation, a DF aggregate over a Lucene-delegated filter would produce
     * alternatives where operator backend ≠ annotation backend — this assertion will need
     * to be relaxed or split per delegation strategy.
     */
    public void testComposedPipelineForking() {
        RelNode pipeline = makeAggregate(
            makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            sumCall()
        );
        QueryDAG dag = buildAndFork(1, pipeline);
        // Aggregate splits into FINAL (coord) + PARTIAL (data node). Each side gets its
        // own stage; both stages get two alternatives during forking. The data-node
        // PARTIAL stage's pipeline includes Aggregate → Filter → Scan (no FINAL above).
        Stage partialStage = findStageWithFragment(dag, OpenSearchAggregate.class);
        assertTwoAlternatives(partialStage, OpenSearchAggregate.class);

        // Each alternative's full pipeline must be narrowed to the same single backend
        for (StagePlan plan : partialStage.getPlanAlternatives()) {
            assertPipelineViableBackends(
                plan.resolvedFragment(),
                List.of(OpenSearchAggregate.class, OpenSearchFilter.class, OpenSearchTableScan.class),
                Set.of(plan.backendId())
            );
        }
    }

    /** Multi-shard aggregate — child stage gets two alternatives, root gets one (only DF has ExchangeSinkProvider). */
    public void testMultiShardAggregateForksAllStages() {
        QueryDAG dag = buildAndFork(2, makeAggregate(sumCall()));

        // Root coordinator stage — only DF has ExchangeSinkProvider
        assertEquals(1, dag.rootStage().getPlanAlternatives().size());

        // Child data node stage — two alternatives
        Stage child = dag.rootStage().getChildStages().getFirst();
        assertTwoAlternatives(child, OpenSearchAggregate.class);
    }

    /**
     * Mixed aggregate: SUM(size), COUNT(*), SUM(status) — COUNT(*) has no field args.
     * All three get annotated. Verifies forking handles the mix without index misalignment.
     */
    public void testMixedAggCallsWithAndWithoutFieldArgs() {
        QueryDAG dag = buildAndFork(
            1,
            makeMultiCallAggregate(
                sumCall(),
                countStarCall(),
                AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    List.of(0),
                    0,
                    stubScan(mockTable("test_index", "status", "size")),
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    "total_status"
                )
            )
        );
        assertTwoAlternatives(findStageWithFragment(dag, OpenSearchAggregate.class), OpenSearchAggregate.class);
    }

    /**
     * Filter with AND of two annotated predicates — verifies tree-walk annotation
     * collection and replacement are consistent across multiple predicates.
     */
    public void testFilterWithMultipleAnnotatedPredicates() {
        QueryDAG dag = buildAndFork(
            1,
            LogicalFilter.create(
                stubScan(mockTable("test_index", "status", "size")),
                makeAnd(
                    makeEquals(0, SqlTypeName.INTEGER, 200),
                    makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                        rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
                    )
                )
            )
        );
        assertTwoAlternatives(findStageWithFragment(dag, OpenSearchFilter.class), OpenSearchFilter.class);
    }

    /**
     * Constant predicate (1=1) must be eliminated by ReduceExpressionsRule before marking.
     * The filter disappears entirely — the root of the marked tree is the scan, not a filter.
     */
    public void testConstantPredicateEliminated() {
        var context = buildContext("parquet", 1, intFields());
        RexNode constant = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), constant);
        RelNode result = runPlanner(filter, context);
        // ReduceExpressionsRule folds 1=1 → TRUE, then filter on TRUE is removed.
        // Scans always declare RANDOM, so the root requires SINGLETON which Volcano
        // satisfies by inserting an ER on top of the scan — the elimination is verified
        // by the scan being the ER's only child rather than by the root type.
        assertFalse("filter on constant true must be eliminated", result instanceof OpenSearchFilter);
        RelNode underRoot = result instanceof org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer er ? er.getInput() : result;
        assertTrue("scan must be directly under any top-level gather after filter elimination", underRoot instanceof OpenSearchTableScan);
    }
}
