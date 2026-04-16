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
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.exec.stage.ShardScanStageExecution;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
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
import java.util.function.Function;

import static org.mockito.Mockito.mock;

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
     * Creates the test index and triggers a refresh so the mock parquet reader
     * registers. Uses clickbench field names because
     * {@code DatafusionReaderManager} serves {@code clickbench_hits_100.parquet}
     * (clickbench schema) via its {@code [indexing-mock]} fallback — the fictional
     * mock schema these tests originally used no longer exists.
     *
     * <p>The mock-parquet fallback only fires on {@code afterRefresh(didRefresh=true)},
     * which requires at least one indexed document followed by a refresh. Without
     * this, {@code DatafusionReaderManager.getReader} throws "No DataFusion reader
     * available" and fragment execution fails at the shard.
     */
    private void createTestIndex() {
        if (indexExists(TEST_INDEX)) {
            return;
        }
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        )
            .setMapping(
                "Age",
                "type=short",
                "AdvEngineID",
                "type=short",
                "RegionID",
                "type=integer",
                "Title",
                "type=keyword",
                "URL",
                "type=keyword"
            )
            .get();
        ensureGreen(TEST_INDEX);

        // Index dummy docs and refresh so each shard fires afterRefresh(didRefresh=true),
        // which triggers the DatafusionReaderManager mock-parquet fallback and registers
        // a reader pointing at clickbench_hits_100.parquet. The dummy doc payload is
        // irrelevant — only the refresh matters. Match the pattern from
        // AnalyticsCoordinatorReduceIT exactly.
        for (int i = 0; i < 4; i++) {
            client().prepareIndex(TEST_INDEX)
                .setId(String.valueOf(i))
                .setSource("Age", 30, "AdvEngineID", 0, "RegionID", 1, "Title", "t", "URL", "u")
                .get();
        }
        client().admin().indices().prepareRefresh(TEST_INDEX).get();
    }

    /**
     * Scan + project: fields Title, URL → 100 rows, 2 columns from clickbench mock data.
     */
    public void testFieldsProjection() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | fields Title, URL");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 2 columns", 2, response.getColumns().size());
        assertTrue("Columns should contain 'Title'", response.getColumns().contains("Title"));
        assertTrue("Columns should contain 'URL'", response.getColumns().contains("URL"));
        assertEquals("Should have 100 rows from clickbench mock parquet data", 100, response.getRows().size());
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
     * SUM(Age) → 4275 for a single shard of clickbench_hits_100.parquet.
     * Matches the per-shard sum constant from {@code AnalyticsCoordinatorReduceIT}.
     */
    public void testSumAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats sum(Age) as total_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'total_age'", response.getColumns().contains("total_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("total_age");
        long totalAge = ((Number) response.getRows().get(0)[idx]).longValue();
        assertEquals("SUM(Age) should be 4275 for 1 shard of clickbench_hits_100.parquet", 4275, totalAge);
    }

    /**
     * MIN/MAX(Age) — tolerant assertion since exact min/max of clickbench_hits_100.parquet
     * Age column isn't published. Verifies the query runs, returns one row, and the
     * min/max bracket is valid (min ≤ max, both non-negative).
     */
    public void testMinMaxAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats min(Age) as min_age, max(Age) as max_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'min_age'", response.getColumns().contains("min_age"));
        assertTrue("Columns should contain 'max_age'", response.getColumns().contains("max_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int minIdx = response.getColumns().indexOf("min_age");
        int maxIdx = response.getColumns().indexOf("max_age");
        long minAge = ((Number) response.getRows().get(0)[minIdx]).longValue();
        long maxAge = ((Number) response.getRows().get(0)[maxIdx]).longValue();
        assertTrue("MIN(Age) should be >= 0, was " + minAge, minAge >= 0);
        assertTrue("MAX(Age) should be >= MIN(Age), max=" + maxAge + " min=" + minAge, maxAge >= minAge);
    }

    /**
     * COUNT(*) GROUP BY AdvEngineID — tolerant assertion: exact group distribution
     * isn't published; we assert the query runs, returns ≥ 1 group, and each group
     * count is positive and bounded by the total row count (100).
     */
    public void testCountGroupBy() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt by AdvEngineID");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'cnt'", response.getColumns().contains("cnt"));
        assertTrue("Columns should contain 'AdvEngineID'", response.getColumns().contains("AdvEngineID"));
        assertTrue("Should have ≥ 1 AdvEngineID group, got " + response.getRows().size(), response.getRows().size() >= 1);

        int cntIdx = response.getColumns().indexOf("cnt");
        long totalFromGroups = 0;
        for (Object[] row : response.getRows()) {
            long cnt = ((Number) row[cntIdx]).longValue();
            assertTrue("Each group count should be > 0, got " + cnt, cnt > 0);
            assertTrue("Each group count should be <= 100, got " + cnt, cnt <= 100);
            totalFromGroups += cnt;
        }
        assertEquals("Sum of group counts should equal total rows (100)", 100, totalFromGroups);
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
     * Builds a {@link ShardScanStageExecution} with a single target and a controllable dispatcher.
     * Uses a real {@link AnalyticsQueryTask} as the parentTask so that
     * {@code finishStageInternal()} can check {@code parentTask.isCancelled()}.
     */
    private ShardScanStageExecution buildBlockingStageExec(
        AnalyticsQueryTask parentTask,
        ShardTransportDispatcher dispatcher,
        ActionListener<Void> listener
    ) {
        Stage stage = new Stage(0, null, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(null, "mock-backend", new byte[0])));

        List<ShardTarget> targets = List.of(mockTarget());
        List<FragmentExecutionRequest.PlanAlternative> planAlts = List.of(
            new FragmentExecutionRequest.PlanAlternative("mock-backend", new byte[0])
        );
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            "test-query",
            stage.getStageId(),
            target.shardId(),
            planAlts
        );

        QueryContext config = new QueryContext(
            new org.opensearch.analytics.planner.dag.QueryDAG("test-query", stage),
            Runnable::run,
            parentTask
        );

        return new ShardScanStageExecution(stage, config, new RowProducingSink(), targets, requestBuilder, dispatcher);
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

            // Dispatcher that blocks on the latch — simulates a slow shard response
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
            AnalyticsSearchTransportService blockingDispatcher = new ShardTransportDispatcher(mock(TransportService.class), clusterService) {
                @Override
                public void dispatchScan(
                    FragmentExecutionRequest request,
                    DiscoveryNode node,
                    StreamingResponseListener<ScanResponse> listener,
                    Task parentTaskArg
                ) {
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
                }
            };

            ShardScanStageExecution stageExecution = buildBlockingStageExec(queryTask, blockingDispatcher, stageFuture);
            stageExecution.start();

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

            // Dispatcher that blocks for a long time — timeout should fire before it completes
            final ActionListener<Void> finalStageListener = stageListener;
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
            ShardTransportDispatcher slowDispatcher = new ShardTransportDispatcher(mock(TransportService.class), clusterService) {
                @Override
                public void dispatchScan(
                    FragmentExecutionRequest request,
                    DiscoveryNode node,
                    StreamingResponseListener<ScanResponse> listener,
                    Task parentTaskArg
                ) {
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
                }
            };

            ShardScanStageExecution stageExecution = buildBlockingStageExec(queryTask, slowDispatcher, finalStageListener);

            long startNanos = System.nanoTime();
            stageExecution.start();

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

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | fields Title, URL");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 2 columns", 2, response.getColumns().size());
        assertTrue("Columns should contain 'Title'", response.getColumns().contains("Title"));
        assertTrue("Columns should contain 'URL'", response.getColumns().contains("URL"));
        assertEquals("Should have 100 rows from clickbench mock parquet data", 100, response.getRows().size());
    }
}
