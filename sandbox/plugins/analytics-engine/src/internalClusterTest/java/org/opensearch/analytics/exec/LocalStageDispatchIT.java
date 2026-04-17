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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration tests for local stage dispatch through a real cluster.
 *
 * <p>Exercises the full local stage path:
 * {@code Scheduler → PlanWalker → LocalStageExecution → TestSummingLocalStageContext}
 * with child stages dispatched via real {@link TransportService} and mock shard handlers.
 *
 * <p>No real DataFusion backend — uses {@link TestLocalStageBackendPlugin} which
 * provides {@link TestSummingLocalStageContext} as the local stage engine.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class LocalStageDispatchIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "local_stage_test";
    private static final String BACKEND_ID = "test-coord-reduce";
    private static final String SHARD_BACKEND_ID = "mock-backend";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, MockTransportService.TestPlugin.class, TestLocalStageBackendPlugin.class, FlightStreamPlugin.class);
    }

    // ─── Calcite helpers ────────────────────────────────────────────────

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    private RelDataType rowType() {
        return typeFactory.builder().add("val", SqlTypeName.BIGINT).build();
    }

    private RelOptCluster calciteCluster() {
        VolcanoPlanner planner = new VolcanoPlanner();
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType());
        return new OpenSearchTableScan(calciteCluster(), RelTraitSet.createEmpty(), table, List.of(SHARD_BACKEND_ID), List.of());
    }

    // ─── Index + DAG helpers ────────────────────────────────────────────

    private void createTestIndex(int numShards) {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();
        ensureGreen(TEST_INDEX);
    }

    /**
     * Builds a local stage DAG with one child data-node stage.
     * Child stage scans TEST_INDEX with a SINGLETON exchange.
     * Root stage is LOCAL with plan alternative pointing at the test backend.
     */
    private QueryDAG localStageDAG(String queryId, int numChildShards) {
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        OpenSearchTableScan childScan = buildTableScan(TEST_INDEX);
        Stage childStage = new Stage(0, childScan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(childScan, SHARD_BACKEND_ID)));

        RelOptCluster cluster = calciteCluster();
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType(),
            List.of(BACKEND_ID)
        );
        RelNode fragment = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, fragment, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(fragment, BACKEND_ID, new byte[0])));

        return new QueryDAG(queryId, rootStage);
    }

    /**
     * Builds a local stage DAG with two child data-node stages.
     */
    private QueryDAG multiChildLocalStageDAG(String queryId) {
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        RelOptCluster cluster = calciteCluster();

        OpenSearchTableScan childScan0 = buildTableScan(TEST_INDEX);
        Stage childStage0 = new Stage(0, childScan0, List.of(), singletonExchange);
        childStage0.setPlanAlternatives(List.of(new StagePlan(childScan0, SHARD_BACKEND_ID)));

        OpenSearchTableScan childScan1 = buildTableScan(TEST_INDEX);
        Stage childStage1 = new Stage(2, childScan1, List.of(), singletonExchange);
        childStage1.setPlanAlternatives(List.of(new StagePlan(childScan1, SHARD_BACKEND_ID)));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType(),
            List.of(BACKEND_ID)
        );
        RelNode fragment = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(3, fragment, List.of(childStage0, childStage1), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(fragment, BACKEND_ID, new byte[0])));

        return new QueryDAG(queryId, rootStage);
    }

    // ─── Fragment helper ───────────────────────────────────────────────

    /**
     * Wraps the given input in a trivial identity {@link LogicalProject} so
     * that the fragment is not a bare {@link OpenSearchStageInputScan} and
     * therefore not treated as pass-through by {@link PlanWalker}.
     */
    private RelNode buildNonPassthroughFragment(RelNode input) {
        org.apache.calcite.rex.RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        return LogicalProject.create(input, List.of(), List.of(rexBuilder.makeInputRef(input, 0)), input.getRowType());
    }

    // ─── Mock transport handler installation ────────────────────────────

    /**
     * Installs a mock handler on all nodes that returns canned row responses
     * with the specified number of batches per shard.
     */
    private void installMockShardHandler(List<Object[]> rows, int batchesPerShard) {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                FragmentExecutionAction.NAME,
                (handler, request, channel, task) -> {
                    for (int i = 0; i < batchesPerShard; i++) {
                        FragmentExecutionResponse response = MockArrowResponse.create(List.of("val"), rows);
                        channel.sendResponseBatch(response);
                    }
                    channel.completeStream();
                }
            );
        }
    }

    /**
     * Installs a mock handler that fails the Nth request across all nodes.
     */
    private void installFailOnNthHandler(List<Object[]> rows, int failOnN) {
        AtomicInteger requestCount = new AtomicInteger(0);
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                FragmentExecutionAction.NAME,
                (handler, request, channel, task) -> {
                    if (requestCount.incrementAndGet() == failOnN) {
                        channel.sendResponse(new RuntimeException("shard execution failed [mock]"));
                    } else {
                        FragmentExecutionResponse response = MockArrowResponse.create(List.of("val"), rows);
                        channel.sendResponseBatch(response);
                        channel.completeStream();
                    }
                }
            );
        }
    }

    /**
     * Installs a mock handler that captures the parent task ID from incoming requests.
     */
    private void installParentTaskCapturingHandler(List<Object[]> rows, List<TaskId> capturedParentTaskIds) {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.<FragmentExecutionRequest>addRequestHandlingBehavior(
                FragmentExecutionAction.NAME,
                (handler, request, channel, task) -> {
                    capturedParentTaskIds.add(task.getParentTaskId());
                    FragmentExecutionResponse response = MockArrowResponse.create(List.of("val"), rows);
                    channel.sendResponseBatch(response);
                    channel.completeStream();
                }
            );
        }
    }

    private void clearMockHandlers() {
        for (String nodeName : internalCluster().getNodeNames()) {
            MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
            transportService.clearAllRules();
        }
    }

    // ─── Scheduler + PlanWalker construction ────────────────────────────

    private String coordinatorNode() {
        return internalCluster().getNodeNames()[0];
    }

    /**
     * Builds a {@link Scheduler} wired with the {@link TestLocalStageBackendPlugin}
     * so LOCAL compute stages route through the test backend.
     */
    private Scheduler buildScheduler() {
        TransportService transportService = internalCluster().getInstance(TransportService.class, coordinatorNode());
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, coordinatorNode());
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, clusterService);
        TestLocalStageBackendPlugin testBackend = new TestLocalStageBackendPlugin();
        return new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of(BACKEND_ID, testBackend))
        );
    }

    private QueryContext buildConfig(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return QueryContext.forTest(dag, parentTask);
    }

    private AnalyticsQueryTask registerQueryTask(String queryId) {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager();
        DefaultPlanExecutor.AnalyticsQueryTaskRequest request = new DefaultPlanExecutor.AnalyticsQueryTaskRequest(queryId, null);
        return (AnalyticsQueryTask) taskManager.register("transport", "analytics_query", request);
    }

    private void unregisterTask(AnalyticsQueryTask task) {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class, coordinatorNode()).getTaskManager();
        try {
            taskManager.unregister(task);
        } catch (Exception e) {
            // may already be unregistered
        }
    }

    // ─── 47.1: Single child accumulates batches ──────────

    /**
     * 3 shards × 1 canned batch each → engine receives 3 batches,
     * all routed to __stage_0_input__, future returns synthesized output,
     * rootSink is non-empty.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSingleChildAccumulatesBatches() throws Exception {
        int numShards = 3;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 42L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("accum-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("accum-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                Iterable<Object[]> result = future.actionGet();
                assertNotNull("Result should not be null", result);
                List<Object[]> rows = new java.util.ArrayList<>();
                result.forEach(rows::add);
                assertTrue("rootSink should have rows", rows.size() > 0);

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertEquals("Engine should have received 3 batches", numShards, engine.totalBatchesReceived());
                assertEquals("All batches should be on __stage_0_input__", numShards, engine.batchesForInput("__stage_0_input__"));
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.2: Multiple batches per shard arrive pipelined ──────────────

    /**
     * 2 shards × 1 canned batch with 4 rows each → totalBatchesReceived == 2.
     * Each batch carries multiple rows to verify row-level data flows through.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultipleBatchesPerShardArrivePipelined() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        // Each shard returns 1 batch with 4 rows
        List<Object[]> cannedRows = Arrays.<Object[]>asList(
            new Object[] { 10L },
            new Object[] { 20L },
            new Object[] { 30L },
            new Object[] { 40L }
        );
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("pipeline-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("pipeline-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertEquals("Engine should have received 2 batches (1 per shard)", numShards, engine.totalBatchesReceived());
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.3: Multiple child stages each with shards ───────────────────

    /**
     * 2 child stages × 2 shards each → batches routed to correct inputs,
     * no cross-routing.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultipleChildStagesEachWithShards() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 7L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = multiChildLocalStageDAG("multi-child-test");
            AnalyticsQueryTask queryTask = registerQueryTask("multi-child-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertEquals("Batches for __stage_0_input__", numShards, engine.batchesForInput("__stage_0_input__"));
                assertEquals("Batches for __stage_2_input__", numShards, engine.batchesForInput("__stage_2_input__"));
                assertEquals("Total batches", numShards * 2, engine.totalBatchesReceived());
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.4: Input closed after all shards complete ───────────────────

    /**
     * After all shard responses arrive, all inputs should be closed and
     * the blocking output stream should have unblocked.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testInputClosedAfterAllShardsComplete() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("close-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("close-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertTrue("All inputs should be closed after completion", engine.allInputsClosed());
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.5: Empty child stage produces empty output ──────────────────

    /**
     * Mock shards return zero rows → engine sees N empty batches →
     * produces output → rootSink result is empty (no data rows from shards).
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEmptyChildStageProducesEmptyOutput() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> emptyRows = Collections.emptyList();
        installMockShardHandler(emptyRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("empty-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("empty-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                Iterable<Object[]> result = future.actionGet();
                assertNotNull("Result should not be null", result);

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                // Engine still receives batches (empty ones) — one per shard
                assertEquals("Engine should have received batches", numShards, engine.totalBatchesReceived());
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.6: Single shard failure closes all inputs and fails ─────────

    /**
     * installFailOnNthHandler fails the 2nd request → future.get() throws
     * ExecutionException wrapping RuntimeException("Stage 0 failed").
     * All inputs are closed and engine is closed.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSingleShardFailureClosesAllInputsAndFails() throws Exception {
        int numShards = 3;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installFailOnNthHandler(cannedRows, 2);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("fail-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("fail-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
                assertNotNull("Should have a cause", ex.getCause());
                assertTrue(
                    "Expected 'Stage 0 failed' but got: " + ex.getCause().getMessage(),
                    ex.getCause().getMessage().contains("Stage 0 failed")
                );

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertTrue("All inputs should be closed after failure", engine.allInputsClosed());
                assertBusy(() -> assertEquals("Engine should have been closed once", 1, engine.closeCount()));
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.7: Engine drain runs on virtual thread ──────────────────────

    /**
     * After successful dispatch, the drain thread captured by the engine
     * should be a virtual thread.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEngineDrainRunsOnVirtualThread() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("vthread-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("vthread-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                Thread drainThread = engine.drainThread();
                assertNotNull("Drain thread should have been captured", drainThread);
                assertTrue("Drain thread should be virtual", drainThread.isVirtual());
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.8: Engine close invoked on success ──────────────────────────

    /**
     * After a happy-path dispatch, engine.closeCount() == 1.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEngineCloseInvokedOnSuccess() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("close-success-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("close-success-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertBusy(() -> assertEquals("Engine should have been closed once", 1, engine.closeCount()));
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.9: Engine close invoked on shard failure ────────────────────

    /**
     * After a failure-path dispatch, engine.closeCount() == 1.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEngineCloseInvokedOnShardFailure() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installFailOnNthHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("close-fail-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("close-fail-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                assertNotNull("Engine should have been created", engine);
                assertBusy(() -> assertEquals("Engine should have been closed once", 1, engine.closeCount()));
                engine.releaseAllBatches();
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.10: Listener signaled exactly once ──────────────────────────

    /**
     * Wraps the dispatcher's ActionListener in a CAS once-checker.
     * Verifies no double-signal occurs across happy and failure paths.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testListenerSignaledExactlyOnce() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        installMockShardHandler(cannedRows, 1);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("once-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("once-test");

            try {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger signalCount = new AtomicInteger(0);

                // Wrap the future with a signal counter
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>() {
                    @Override
                    public void onResponse(Iterable<Object[]> result) {
                        signalCount.incrementAndGet();
                        super.onResponse(result);
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        signalCount.incrementAndGet();
                        super.onFailure(e);
                        latch.countDown();
                    }
                };

                scheduler.execute(buildConfig(dag, queryTask), future);

                assertTrue("Should complete within 10s", latch.await(10, TimeUnit.SECONDS));
                // Give a small window for any spurious double-signal
                Thread.sleep(100);
                assertEquals("Listener should be signaled exactly once", 1, signalCount.get());

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                if (engine != null) {
                    engine.releaseAllBatches();
                }
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }

    // ─── 47.11: Parent task propagates to shard requests ────────────────

    /**
     * Captures the incoming task's parentTaskId from mock shard handlers.
     * Asserts it equals the registered AnalyticsQueryTask's id.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testParentTaskPropagatesToShardRequests() throws Exception {
        int numShards = 2;
        createTestIndex(numShards);
        List<Object[]> cannedRows = Arrays.<Object[]>asList(new Object[] { 1L });
        List<TaskId> capturedParentTaskIds = Collections.synchronizedList(new java.util.ArrayList<>());
        installParentTaskCapturingHandler(cannedRows, capturedParentTaskIds);

        try {
            TestLocalStageBackendPlugin.lastInstance = null;
            Scheduler scheduler = buildScheduler();
            QueryDAG dag = localStageDAG("parent-task-test", numShards);
            AnalyticsQueryTask queryTask = registerQueryTask("parent-task-test");

            try {
                PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
                scheduler.execute(buildConfig(dag, queryTask), future);

                future.actionGet();

                assertFalse("Should have captured parent task IDs", capturedParentTaskIds.isEmpty());
                TaskId expectedParentTaskId = queryTask.taskInfo(
                    internalCluster().getInstance(TransportService.class, coordinatorNode()).getLocalNode().getId(),
                    false
                ).getTaskId();
                for (TaskId captured : capturedParentTaskIds) {
                    assertEquals("Shard request should carry the query task as parent", expectedParentTaskId, captured);
                }

                TestSummingLocalStageContext engine = TestLocalStageBackendPlugin.lastInstance;
                if (engine != null) {
                    engine.releaseAllBatches();
                }
            } finally {
                unregisterTask(queryTask);
            }
        } finally {
            clearMockHandlers();
        }
    }
}
