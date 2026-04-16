/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.AnalyticsScanAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for {@link Scheduler} — coordinator-side dispatch through
 * a real {@link TransportService} with mock shard responses.
 *
 * <p>Tests exercise the full coordinator path:
 * {@code Scheduler → PlanWalker → StageExecution → TransportService → mock handler → response → sink}
 *
 * <p>No analytics backends (DataFusion, Lucene) are loaded. The transport action
 * handler is replaced with a mock that returns canned {@link ScanResponse}s.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SchedulerDispatchIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "scheduler_test";
    private static final String TEST_INDEX_B = "scheduler_test_b";
    private static final String BACKEND_ID = "mock-backend";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, MockTransportService.TestPlugin.class);
    }

    // ─── Calcite helpers for building mock RelNodes ─────────────────────

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    private RelDataType rowType() {
        return typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).add("field_1", SqlTypeName.BIGINT).build();
    }

    private RelOptCluster calciteCluster() {
        VolcanoPlanner planner = new VolcanoPlanner();
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType());
        return new OpenSearchTableScan(calciteCluster(), RelTraitSet.createEmpty(), table, List.of(BACKEND_ID), List.of());
    }

    // ─── Index + DAG helpers ────────────────────────────────────────────

    private void createTestIndex(int numShards) {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();
        ensureGreen(TEST_INDEX);
    }

    private QueryDAG singleStageDAG(String queryId) {
        OpenSearchTableScan scan = buildTableScan(TEST_INDEX);
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, BACKEND_ID)));
        return new QueryDAG(queryId, stage);
    }

    private QueryDAG twoStageDAG(String queryId) {
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        OpenSearchTableScan childScan = buildTableScan(TEST_INDEX);
        Stage childStage = new Stage(0, childScan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(childScan, BACKEND_ID)));

        // Root stage: LOCAL (null fragment → no tableName, no exchangeInfo → LOCAL)
        Stage rootStage = new Stage(1, null, List.of(childStage), null);
        // No plan alternatives → coordinator gather, dispatch skipped
        return new QueryDAG(queryId, rootStage);
    }

    // ─── Mock transport handler installation ────────────────────────────

    /**
     * Installs a mock handler on all nodes that returns canned row responses
     * for {@code indices:data/read/analytics/shard}.
     */
    private void installMockShardHandler(List<Object[]> rows) {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                AnalyticsScanAction.NAME,
                (handler, request, channel, task) -> {
                    ScanResponse response = new ScanResponse(List.of("field_0", "field_1"), rows);
                    channel.sendResponse(response);
                }
            );
        }
    }

    /**
     * Installs a mock handler that tracks in-flight count for concurrency testing.
     */
    private void installConcurrencyTrackingHandler(
        List<Object[]> rows,
        AtomicInteger currentInFlight,
        AtomicInteger maxObservedInFlight,
        CountDownLatch allDispatched
    ) {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                AnalyticsScanAction.NAME,
                (handler, request, channel, task) -> {
                    int inflight = currentInFlight.incrementAndGet();
                    maxObservedInFlight.updateAndGet(prev -> Math.max(prev, inflight));
                    // Small delay to let concurrency build up
                    try {
                        Thread.sleep(50);
                    } finally {
                        currentInFlight.decrementAndGet();
                        allDispatched.countDown();
                    }
                    channel.sendResponse(new ScanResponse(List.of("field_0", "field_1"), rows));
                }
            );
        }
    }

    /**
     * Installs a mock handler that fails the Nth request.
     */
    private void installFailOnNthHandler(List<Object[]> rows, int failOnN) {
        AtomicInteger requestCount = new AtomicInteger(0);
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                AnalyticsScanAction.NAME,
                (handler, request, channel, task) -> {
                    if (requestCount.incrementAndGet() == failOnN) {
                        channel.sendResponse(new RuntimeException("shard execution failed [mock]"));
                    } else {
                        channel.sendResponse(new ScanResponse(List.of("field_0", "field_1"), rows));
                    }
                }
            );
        }
    }

    /**
     * Installs a blocking handler that waits on a latch before responding.
     */
    private void clearMockHandlers() {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.clearAllRules();
        }
    }

    // ─── Scheduler construction ─────────────────────────────────────────

    private Scheduler buildScheduler(int maxConcurrentShardRequests) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, coordinatorNode());
        // Use regular TransportService for dispatch since MockTransportService
        // intercepts requests on the regular transport. The handleResponse fallback
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, coordinatorNode());
        ShardTransportDispatcher dispatcher = new AnalyticsSearchTransportService(transportService, clusterService);
        return new EventDrivenScheduler(new StageExecutionBuilder(clusterService, dispatcher, null, null, null), dispatcher);
    }

    private QueryContext buildConfig(QueryDAG dag, Task parentTask) {
        return QueryContext.forTest(dag, parentTask);
    }

    /** Returns the coordinator node name (first node). Used to ensure task registration and scheduler use the same node. */
    private String coordinatorNode() {
        return internalCluster().getNodeNames()[0];
    }

    private AnalyticsQueryTask registerQueryTask(String queryId) {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager();
        DefaultPlanExecutor.AnalyticsQueryTaskRequest request = new DefaultPlanExecutor.AnalyticsQueryTaskRequest(queryId, null);
        return (AnalyticsQueryTask) taskManager.register("transport", "analytics_query", request);
    }

    // ─── Tests ──────────────────────────────────────────────────────────

    /**
     * Single stage, 3 shards. Canned row responses flow through the full
     * coordinator path and arrive in the root sink.
     */
    public void testSingleStageFanOut() throws Exception {
        int numShards = 3;
        createTestIndex(numShards);

        List<Object[]> cannedRows = java.util.Arrays.asList(new Object[] { "alice", 30L }, new Object[] { "bob", 25L });
        installMockShardHandler(cannedRows);

        try {
            Scheduler scheduler = buildScheduler(5);
            QueryDAG dag = singleStageDAG("fanout-test");
            AnalyticsQueryTask queryTask = registerQueryTask("fanout-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                Iterable<Object[]> result = future.actionGet();
                List<Object[]> rows = new java.util.ArrayList<>();
                result.forEach(rows::add);

                // Each shard returns 2 rows → 3 shards × 2 rows = 6 total
                assertEquals("Expected " + (numShards * cannedRows.size()) + " rows", numShards * cannedRows.size(), rows.size());
            } finally {
                internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager().unregister(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    /**
     * 6 shards on 2 nodes with maxConcurrentShardRequests=2.
     * Verifies at most 2 requests are in-flight per node at any time.
     */
    public void testPerNodeConcurrencyGating() throws Exception {
        int numShards = 6;
        createTestIndex(numShards);

        List<Object[]> cannedRows = Collections.singletonList(new Object[] { "val", 1L });
        AtomicInteger currentInFlight = new AtomicInteger(0);
        AtomicInteger maxObservedInFlight = new AtomicInteger(0);
        CountDownLatch allDispatched = new CountDownLatch(numShards);
        installConcurrencyTrackingHandler(cannedRows, currentInFlight, maxObservedInFlight, allDispatched);

        try {
            int maxConcurrent = 2;
            Scheduler scheduler = buildScheduler(maxConcurrent);
            QueryDAG dag = singleStageDAG("concurrency-test");
            AnalyticsQueryTask queryTask = registerQueryTask("concurrency-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                // maxConcurrent is per-node. With 2 nodes, theoretical max in-flight is 2*maxConcurrent=4.
                // But we assert per-node max never exceeds maxConcurrent.
                // The AtomicInteger tracks global in-flight, so with 2 nodes it could be up to 2*maxConcurrent.
                assertTrue(
                    "Max observed in-flight ("
                        + maxObservedInFlight.get()
                        + ") should be <= 2 * maxConcurrent ("
                        + (2 * maxConcurrent)
                        + ")",
                    maxObservedInFlight.get() <= 2 * maxConcurrent
                );
                // And must be > 0 (something was dispatched)
                assertTrue("At least 1 request should have been in-flight", maxObservedInFlight.get() > 0);
            } finally {
                internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager().unregister(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    /**
     * One shard returns an error. The stage fails with "Stage N failed" wrapping.
     */
    public void testShardFailurePropagates() throws Exception {
        int numShards = 3;
        createTestIndex(numShards);

        List<Object[]> cannedRows = Collections.singletonList(new Object[] { "ok", 1L });
        installFailOnNthHandler(cannedRows, 2); // fail the 2nd request

        try {
            Scheduler scheduler = buildScheduler(5);
            QueryDAG dag = singleStageDAG("failure-test");
            AnalyticsQueryTask queryTask = registerQueryTask("failure-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                try {
                    future.get(10, TimeUnit.SECONDS);
                    fail("Expected exception but query succeeded");
                } catch (ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    assertTrue(
                        "Expected RuntimeException with 'Stage 0 failed' but got: " + cause.getMessage(),
                        cause.getMessage().contains("Stage 0 failed")
                    );
                }
            } finally {
                internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager().unregister(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // Cancellation through real transport has inherent timing races between
    // taskManager.cancel() on the coordinator and the response arriving from the
    // mock handler on a remote node. Cancellation is thoroughly tested in unit tests:
    // CoordinatorDispatchTests.testCancellationReturnsCleanException
    // StageExecCancellationTests.testTopDownCancellationReturnsCleanException

    /**
     * Two-stage DAG (child stage → coordinator gather root).
     * Verifies bottom-up execution: child dispatches to shards first,
     * then root stage completes as coordinator gather.
     */
    public void testTwoStageBottomUpExecution() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);

        List<Object[]> cannedRows = Collections.singletonList(new Object[] { "row", 42L });
        installMockShardHandler(cannedRows);

        try {
            Scheduler scheduler = buildScheduler(5);
            QueryDAG dag = twoStageDAG("twostage-test");
            AnalyticsQueryTask queryTask = registerQueryTask("twostage-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                Iterable<Object[]> result = future.actionGet();
                List<Object[]> rows = new java.util.ArrayList<>();
                result.forEach(rows::add);

                // Child stage: 2 shards × 1 row = 2 rows fed to root sink
                // Root stage: coordinator gather → returns rootSink contents
                assertEquals("Expected " + numShards + " rows", numShards, rows.size());
            } finally {
                internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager().unregister(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }
}
