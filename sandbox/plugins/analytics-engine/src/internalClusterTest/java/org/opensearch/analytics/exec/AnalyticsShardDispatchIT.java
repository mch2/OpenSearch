/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end integration test for the analytics shard dispatch pipeline
 * using the DataFusion backend with mock parquet data.
 *
 * <p>Exercises the full pipeline: PPL parsing → Calcite planning → backend marking →
 * DAG construction → Substrait fragment conversion → shard dispatch → DataFusion
 * native execution → Arrow result collection → PPL response.
 *
 * <p>DataFusion's mock parquet reader serves 100 rows of pre-generated data
 * (id, name, age, score, city) regardless of index contents. The index only
 * needs to exist for schema registration.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class AnalyticsShardDispatchIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AnalyticsShardDispatchIT.class);
    private static final String TEST_INDEX = "parquet_simple";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                AnalyticsPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                AnalyticsPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            // Parquet data format plugin — its loadExtensions() registers ParquetDataFormat
            // with the DataFusion backend (via DataFusionDataFormatExtension SPI). Without this,
            // DataFusionPlugin.supportedFormats is empty and the planner errors with
            // "Field [...] has no storage in any format". See AnalyticsCoordinatorReduceIT for
            // the rationale on why parquet has empty extendedPlugins in tests.
            new PluginInfo(
                org.opensearch.parquet.ParquetDataFormatPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                org.opensearch.parquet.ParquetDataFormatPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            // DataFusion plugin extends BOTH analytics-engine AND parquet-data-format so its
            // SPI-discovered extensions (DataFusionAnalyticsExtension, DataFusionDataFormatExtension)
            // are picked up by the right loadExtensions() call sites.
            new PluginInfo(
                DataFusionPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                DataFusionPlugin.class.getName(),
                null,
                List.of(AnalyticsPlugin.class.getName(), org.opensearch.parquet.ParquetDataFormatPlugin.class.getName()),
                false
            )
        );
    }

    /**
     * Creates the test index with the schema matching DataFusion's mock parquet data.
     * No data ingestion needed — DataFusion's mock reader serves 100 pre-generated rows.
     */
    private void createTestIndex() {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("id", "type=long", "name", "type=keyword", "age", "type=long", "score", "type=long", "city", "type=keyword").get();
        ensureGreen(TEST_INDEX);
    }

    /**
     * Scan + project: fields name, city → 100 rows, 2 columns from DataFusion mock data.
     */
    public void testFieldsProjection() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | fields name, city");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 2 columns", 2, response.getColumns().size());
        assertTrue("Columns should contain 'name'", response.getColumns().contains("name"));
        assertTrue("Columns should contain 'city'", response.getColumns().contains("city"));
        assertEquals("Should have 100 rows from mock parquet data", 100, response.getRows().size());
    }

    /**
     * COUNT(*) → 100 (DataFusion's mock parquet has 100 rows).
     */
    public void testCountAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'cnt'", response.getColumns().contains("cnt"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int cntIdx = response.getColumns().indexOf("cnt");
        long cnt = ((Number) response.getRows().get(0)[cntIdx]).longValue();
        assertEquals("COUNT should be 100", 100, cnt);
    }

    /**
     * SUM(age) → 4228 (known sum from DataFusion's mock parquet data).
     */
    public void testSumAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats sum(age) as total_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'total_age'", response.getColumns().contains("total_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("total_age");
        long totalAge = ((Number) response.getRows().get(0)[idx]).longValue();
        assertEquals("SUM(age) should be 4228", 4228, totalAge);
    }

    /**
     * MIN/MAX age → min=18, max=65 from DataFusion's mock parquet data.
     */
    public void testMinMaxAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats min(age) as min_age, max(age) as max_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'min_age'", response.getColumns().contains("min_age"));
        assertTrue("Columns should contain 'max_age'", response.getColumns().contains("max_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int minIdx = response.getColumns().indexOf("min_age");
        int maxIdx = response.getColumns().indexOf("max_age");
        assertEquals("MIN(age) should be 18", 18, ((Number) response.getRows().get(0)[minIdx]).longValue());
        assertEquals("MAX(age) should be 65", 65, ((Number) response.getRows().get(0)[maxIdx]).longValue());
    }

    /**
     * COUNT(*) GROUP BY city → 5 cities with known counts from mock parquet data.
     */
    public void testCountGroupBy() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt by city");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'cnt'", response.getColumns().contains("cnt"));
        assertTrue("Columns should contain 'city'", response.getColumns().contains("city"));
        assertEquals("Should have 5 city groups", 5, response.getRows().size());

        int cntIdx = response.getColumns().indexOf("cnt");
        int cityIdx = response.getColumns().indexOf("city");

        // Collect results into a map for flexible assertion
        java.util.Map<String, Long> cityCounts = new java.util.HashMap<>();
        for (Object[] row : response.getRows()) {
            String city = String.valueOf(row[cityIdx]);
            long cnt = ((Number) row[cntIdx]).longValue();
            cityCounts.put(city, cnt);
        }

        assertEquals("paris count", 12, (long) cityCounts.get("paris"));
        assertEquals("tokyo count", 22, (long) cityCounts.get("tokyo"));
        assertEquals("berlin count", 26, (long) cityCounts.get("berlin"));
        assertEquals("new york count", 22, (long) cityCounts.get("new york"));
        assertEquals("london count", 18, (long) cityCounts.get("london"));
    }

    // ─── Cancellation / Timeout integration tests ───────────────────────

    /**
     * Registers an {@link AnalyticsQueryTask} with the cluster's TaskManager.
     * Returns the registered task. Caller is responsible for unregistering.
     */
    private AnalyticsQueryTask registerQueryTask(String queryId, TimeValue cancelAfterTimeInterval) {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        DefaultPlanExecutor.AnalyticsQueryTaskRequest request = new DefaultPlanExecutor.AnalyticsQueryTaskRequest(
            queryId,
            cancelAfterTimeInterval
        );
        Task task = taskManager.register("transport", "analytics_query", request);
        return (AnalyticsQueryTask) task;
    }

    /**
     * Creates a single mock {@link ShardTarget} pointing to the first data node in the cluster.
     */
    private ShardTarget mockTarget() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        org.opensearch.cluster.node.DiscoveryNode node = clusterService.state().nodes().getDataNodes().values().iterator().next();
        return new ShardTarget(new org.opensearch.core.index.shard.ShardId("_test", "_na_", 0), node);
    }

    /**
     * Builds a {@link FanOutStageExecution} with a single target and a controllable client.
     * Uses a real {@link AnalyticsQueryTask} as the parentTask so that
     * {@code finishStageInternal()} can check {@code parentTask.isCancelled()}.
     */
    private FanOutStageExecution buildBlockingStageExec(
        AnalyticsQueryTask parentTask,
        ShardRequestClient client,
        ActionListener<Void> listener
    ) {
        Stage stage = new Stage(0, null, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(null, "mock-backend", new byte[0])));

        List<ShardTarget> targets = List.of(mockTarget());
        List<FragmentExecutionRequest.PlanAlternative> planAlts = List.of(
            new FragmentExecutionRequest.PlanAlternative("mock-backend", new byte[0])
        );

        QueryState state = new QueryState(new SimpleExchangeSink());

        return new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            planAlts,
            Runnable::run,
            parentTask,
            state.rootSink(),
            new SinkFeedingHandler(new SimpleExchangeSink()),
            state.completedStages(),
            state.shuffleManifests(),
            client,
            listener,
            new StageMetrics(stage.getStageId())
        );
    }

    /**
     * 16.1: Top-down cancellation propagates to shards.
     *
     * Registers a real AnalyticsQueryTask with the cluster's TaskManager, constructs
     * a StageExecution with a blocking ShardRequestClient, starts execution, waits for the
     * query to be in-flight via CountDownLatch, cancels the task via TaskManager,
     * and asserts the query fails with TaskCancelledException (not "Stage N failed").
     *
     * Requirements: 1.1, 3.1
     */
    public void testTopDownCancellationPropagatesToShards() throws Exception {
        createTestIndex();

        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();

        String queryId = "cancel-test-" + randomAlphaOfLength(8);
        AnalyticsQueryTask queryTask = registerQueryTask(queryId, null);

        try {
            // Latch to signal that the client has received a request (query is in-flight)
            CountDownLatch inFlightLatch = new CountDownLatch(1);
            // Latch to hold the client until we cancel the task
            CountDownLatch blockLatch = new CountDownLatch(1);

            PlainActionFuture<Void> stageFuture = new PlainActionFuture<>();

            // ShardRequestClient that blocks on the latch — simulates a slow shard response
            ShardRequestClient blockingClient = (request, node, listener) -> {
                // Run in a separate thread so StageExecution.run() returns immediately
                new Thread(() -> {
                    inFlightLatch.countDown();
                    try {
                        boolean released = blockLatch.await(10, TimeUnit.SECONDS);
                        if (released == false) {
                            listener.onFailure(new RuntimeException("blockLatch timed out"));
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        listener.onFailure(new RuntimeException("interrupted", e));
                        return;
                    }
                    // After cancellation, respond with TaskCancelledException
                    // (simulating what a data node would do when its shard task is cancelled)
                    listener.onFailure(new TaskCancelledException("task cancelled [test]"));
                }, "blocking-client").start();
            };

            FanOutStageExecution stageExecution = buildBlockingStageExec(queryTask, blockingClient, stageFuture);
            stageExecution.run();

            // Wait for the query to be in-flight
            assertTrue("Query should become in-flight within 5s", inFlightLatch.await(5, TimeUnit.SECONDS));

            // Cancel the coordinator task — this sets parentTask.isCancelled() = true
            taskManager.cancel(queryTask, "test cancellation", () -> {});

            // Release the blocking submitter so it can respond
            blockLatch.countDown();

            // Assert the stage fails with TaskCancelledException within 5s
            ExecutionException ex = expectThrows(ExecutionException.class, () -> stageFuture.get(5, TimeUnit.SECONDS));
            Throwable cause = ExceptionsHelper.unwrapCause(ex.getCause());
            assertTrue(
                "Expected TaskCancelledException but got: " + cause.getClass().getName() + ": " + cause.getMessage(),
                cause instanceof TaskCancelledException
            );
            // Verify it's the clean "query cancelled" message, NOT "Stage 0 failed"
            assertEquals("query cancelled", cause.getMessage());
        } finally {
            taskManager.unregister(queryTask);
        }
    }

    /**
     * 16.2: Coordinator timeout cancels the query.
     *
     * Registers an AnalyticsQueryTask with cancelAfterTimeInterval=50ms.
     * Constructs a StageExecution with a slow ShardRequestClient. Wraps the stage listener
     * with TimeoutTaskCancellationUtility. Verifies the query fails with
     * TaskCancelledException (NOT "Stage N failed") and completes within a
     * reasonable time (not the full 10s block).
     *
     * Requirements: 2.2, 2.4, 2.6
     */
    public void testCoordinatorTimeoutCancelsQuery() throws Exception {
        createTestIndex();

        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);

        String queryId = "timeout-test-" + randomAlphaOfLength(8);
        TimeValue timeout = TimeValue.timeValueMillis(50);
        AnalyticsQueryTask queryTask = registerQueryTask(queryId, timeout);

        try {
            // Latch to hold the submitter for a long time (simulating slow shard)
            CountDownLatch blockLatch = new CountDownLatch(1);

            PlainActionFuture<Void> stageFuture = new PlainActionFuture<>();

            // Wrap the stage listener with timeout cancellation utility
            ActionListener<Void> stageListener = ActionListener.wrap(v -> stageFuture.onResponse(null), stageFuture::onFailure);

            // TimeoutTaskCancellationUtility wraps the listener and schedules a timer.
            // When the timer fires, it sends a CancelTasksRequest which cancels the task.
            stageListener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                nodeClient,
                queryTask,
                timeout,
                stageListener,
                e -> {}
            );

            // ShardRequestClient that blocks for a long time — timeout should fire before it completes
            final ActionListener<Void> finalStageListener = stageListener;
            ShardRequestClient slowClient = (request, node, listener) -> {
                new Thread(() -> {
                    try {
                        boolean released = blockLatch.await(10, TimeUnit.SECONDS);
                        if (released == false) {
                            listener.onFailure(new RuntimeException("blockLatch timed out"));
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        listener.onFailure(new RuntimeException("interrupted", e));
                        return;
                    }
                    listener.onFailure(new TaskCancelledException("task cancelled [timeout]"));
                }, "slow-client").start();
            };

            FanOutStageExecution stageExecution = buildBlockingStageExec(queryTask, slowClient, finalStageListener);

            long startNanos = System.nanoTime();
            stageExecution.run();

            // Wait for the timeout to fire and the task to be cancelled
            assertBusy(() -> assertTrue("Task should be cancelled by timeout", queryTask.isCancelled()), 5, TimeUnit.SECONDS);

            // Release the blocking submitter so it can respond with cancellation
            blockLatch.countDown();

            // Assert the stage fails with TaskCancelledException
            ExecutionException ex = expectThrows(ExecutionException.class, () -> stageFuture.get(5, TimeUnit.SECONDS));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            Throwable cause = ExceptionsHelper.unwrapCause(ex.getCause());
            assertTrue(
                "Expected TaskCancelledException but got: " + cause.getClass().getName() + ": " + cause.getMessage(),
                cause instanceof TaskCancelledException
            );

            // Verify elapsed time is reasonable — should be much less than 10s (the block timeout)
            logger.info("Coordinator timeout test completed in {}ms", elapsedMs);
            assertTrue("Elapsed time should be less than 5000ms (timeout + overhead), was " + elapsedMs + "ms", elapsedMs < 5000);
        } finally {
            try {
                taskManager.unregister(queryTask);
            } catch (Exception e) {
                // may already be unregistered
            }
        }
    }

    /**
     * 16.3: Normal query without timeout succeeds.
     *
     * Sanity check: runs a normal PPL query with no cancellation or timeout.
     * Verifies it succeeds as before with no behavioral change.
     *
     * Requirements: 5.1
     */
    public void testNormalQueryWithoutTimeoutSucceeds() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | fields name, city");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 2 columns", 2, response.getColumns().size());
        assertTrue("Columns should contain 'name'", response.getColumns().contains("name"));
        assertTrue("Columns should contain 'city'", response.getColumns().contains("city"));
        assertEquals("Should have 100 rows from mock parquet data", 100, response.getRows().size());
    }
}
