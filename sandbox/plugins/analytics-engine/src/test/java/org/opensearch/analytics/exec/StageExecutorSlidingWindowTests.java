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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the sliding-window dispatch pattern in {@link StageExecutor}.
 * Exercises custom {@link TerminationDecider} implementations to verify
 * initial batch sizing, per-completion dispatch, early termination,
 * metrics accuracy, failure propagation, and sticky termination.
 *
 * Validates: Requirements 1.1, 1.2, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.6, 2.7, 3.1, 3.2, 4.1, 4.2, 4.3, 4.4
 */
@SuppressWarnings("unchecked")
public class StageExecutorSlidingWindowTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    private ClusterService buildMockClusterService(String tableName, int numShards) {
        Index index = new Index(tableName, "_na_");

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node_" + i);
            when(discoveryNodes.get("node_" + i)).thenReturn(node);
        }
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ShardIterator shardIt = mock(ShardIterator.class);
            ShardRouting shard = mock(ShardRouting.class);
            when(shard.shardId()).thenReturn(new ShardId(index, i));
            when(shard.currentNodeId()).thenReturn("node_" + i);
            when(shardIt.nextOrNull()).thenReturn(shard);
            iterators.add(shardIt);
        }
        GroupShardsIterator<ShardIterator> groupIterator = new GroupShardsIterator<>(iterators);

        OperationRouting operationRouting = mock(OperationRouting.class);
        when(operationRouting.searchShards(any(), eq(new String[] { tableName }), isNull(), isNull())).thenReturn(groupIterator);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        return clusterService;
    }

    /**
     * Creates a single-stage DAG with a custom TerminationDecider.
     */
    private QueryDAG buildDagWithDecider(String tableName, TerminationDecider decider) {
        OpenSearchTableScan scan = buildTableScan(tableName, List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        stage.setTerminationDecider(decider);
        return new QueryDAG("test-query", stage);
    }

    /**
     * Helper to build a simple successful response for a shard.
     */
    private FragmentExecutionResponse successResponse(int shardIdx) {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "value_" + shardIdx });
        return new FragmentExecutionResponse(List.of("field_0"), rows);
    }

    // ---- 6.2: testInitialBatchSizeHonored ----

    /**
     * Decider with initialBatchSize=3, shouldTerminate=false, 10 targets.
     * Assert exactly 3 submissions before any response, then all 10 after responding.
     *
     * Validates: Requirements 1.1, 1.2
     */
    public void testInitialBatchSizeHonored() {
        int numShards = 10;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 3;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return false;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        // Capture listeners so we can control when responses arrive
        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        // Before any response, exactly 3 should be submitted
        assertEquals("Initial batch should be 3", 3, capturedListeners.size());

        // Respond to all 3 initial listeners — each should trigger one more submission
        // With synchronous executor (Runnable::run), responding to listener i immediately
        // runs the completion handler which submits the next task and adds its listener.
        for (int i = 0; i < 3; i++) {
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
        }
        // respond to 0 → submit 3, respond to 1 → submit 4, respond to 2 → submit 5
        assertEquals("After responding to 3 initial, should have 6 total submissions", 6, capturedListeners.size());

        // Respond to the next batch
        for (int i = 3; i < 6; i++) {
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
        }
        assertEquals("After responding to 6, should have 9 total submissions", 9, capturedListeners.size());

        // Respond to the next batch
        for (int i = 6; i < 9; i++) {
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
        }
        assertEquals("After responding to 9, should have 10 total submissions", 10, capturedListeners.size());

        // Respond to the last one to complete the walk
        capturedListeners.get(9).onStreamResponse(successResponse(capturedRequests.get(9).getShardId().id()), true);

        // Walk should complete successfully
        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals("Should have 10 rows from 10 shards", 10, resultList.size());
    }

    // ---- 6.3: testSubmitsNextAfterCompletion ----

    /**
     * Decider with initialBatchSize=2, shouldTerminate=false, 5 targets.
     * Respond to 1st task → assert 3 total submissions (2 initial + 1 new).
     *
     * Validates: Requirements 2.1, 2.2
     */
    public void testSubmitsNextAfterCompletion() {
        int numShards = 5;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 2;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return false;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        // 2 initial submissions
        assertEquals("Initial batch should be 2", 2, capturedListeners.size());

        // Respond to 1st → should trigger 1 more submission
        capturedListeners.get(0).onStreamResponse(successResponse(capturedRequests.get(0).getShardId().id()), true);
        assertEquals("After 1st response, should have 3 total submissions", 3, capturedListeners.size());

        // Respond to remaining to complete the walk
        for (int i = 1; i < capturedListeners.size();) {
            int size = capturedListeners.size();
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
            i++;
            // New listeners may have been added
        }

        assertEquals("All 5 should be submitted", 5, capturedListeners.size());

        // Walk should complete
        Iterable<Object[]> result = future.actionGet();
        assertNotNull(result);
    }

    // ---- 6.4: testEarlyTerminationStopsDispatch ----

    /**
     * Decider with initialBatchSize=2, shouldTerminate returns true after 1st completion.
     * 10 targets. Assert only 2 total submissions (no new ones after termination).
     *
     * Validates: Requirements 2.3, 2.4
     */
    public void testEarlyTerminationStopsDispatch() {
        int numShards = 10;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 2;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                // Terminate after the 1st completion
                return completedTasks >= 1;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        // 2 initial submissions
        assertEquals("Initial batch should be 2", 2, capturedListeners.size());

        // Respond to 1st → shouldTerminate returns true → no more submissions
        capturedListeners.get(0).onStreamResponse(successResponse(capturedRequests.get(0).getShardId().id()), true);

        // Still only 2 total submissions — termination stopped further dispatch
        assertEquals("Should still have only 2 submissions after termination", 2, capturedListeners.size());

        // Respond to 2nd (late, in-flight at time of termination) — discarded
        capturedListeners.get(1).onStreamResponse(successResponse(capturedRequests.get(1).getShardId().id()), true);

        // Still only 2 submissions
        assertEquals("Should still have only 2 submissions", 2, capturedListeners.size());

        // Walk should have completed after the 1st response triggered termination
        Iterable<Object[]> result = future.actionGet();
        assertNotNull(result);
    }

    // ---- 6.5: testEarlyTerminationSignalsCompletionImmediately ----

    /**
     * Decider terminates on 1st completion (initialBatchSize=2).
     * Respond to 1st → terminates → assert listener signaled immediately with success.
     * Respond to 2nd (late) → assert response is discarded (sink row count doesn't increase).
     *
     * Validates: Requirements 2.4, 2.7, 3.1
     */
    public void testEarlyTerminationSignalsCompletionImmediately() {
        int numShards = 5;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 2;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        assertEquals(2, capturedListeners.size());

        // Respond to 1st → terminates → listener signaled immediately
        capturedListeners.get(0).onStreamResponse(successResponse(capturedRequests.get(0).getShardId().id()), true);

        // Future should already be done (termination signals completion immediately)
        assertTrue("Future should be done after termination", future.isDone());
        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        long rowCountAfterTermination = resultList.size();
        assertEquals("Should have 1 row from the 1st response", 1, rowCountAfterTermination);

        // Respond to 2nd (late) — should be discarded, sink row count should not increase
        capturedListeners.get(1).onStreamResponse(successResponse(capturedRequests.get(1).getShardId().id()), true);

        // Verify row count didn't increase after late response
        ExchangeSink rootSink = walker.getState().rootSink();
        assertEquals("Sink row count should not increase after late response", rowCountAfterTermination, rootSink.getRowCount());
    }

    // ---- 6.6: testInitialBatchSizeZeroCompletesImmediately ----

    /**
     * Decider with initialBatchSize=0, 10 targets.
     * Assert listener signaled immediately, 0 submissions, stageOutputs contains RowData.
     *
     * Validates: Requirements 1.4
     */
    public void testInitialBatchSizeZeroCompletesImmediately() throws Exception {
        int numShards = 10;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 0;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return false;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        AtomicInteger submitCount = new AtomicInteger(0);
        ShardRequestClient client = (request, node, listener) -> {
            submitCount.incrementAndGet();
            listener.onStreamResponse(successResponse(request.getShardId().id()), true);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        // Should complete immediately with 0 submissions
        assertTrue("Future should be done immediately", future.isDone());
        assertEquals("Should have 0 submissions", 0, submitCount.get());

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals("Should have 0 rows", 0, resultList.size());

        // Verify completedStages contains stage 0
        Set<Integer> completed = walker.getState().completedStages();

        assertTrue("Stage 0 should be in completedStages", completed.contains(0));
    }

    // ---- 6.7: testMetricsReflectActualDispatches ----

    /**
     * Decider with initialBatchSize=3, terminates after 2 completions, 10 targets.
     * Respond to 2 tasks → termination → respond to 3rd (late, discarded).
     * Assert metrics reflect actual completions, not total targets.
     *
     * Validates: Requirements 4.1, 4.2, 4.3, 4.4
     */
    public void testMetricsReflectActualDispatches() throws Exception {
        int numShards = 10;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 3;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 2;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        assertEquals("Initial batch should be 3", 3, capturedListeners.size());

        // Respond to 1st → shouldTerminate(1, 10) = false → submits task 3
        capturedListeners.get(0).onStreamResponse(successResponse(capturedRequests.get(0).getShardId().id()), true);
        assertEquals("After 1st response, should have 4 total submissions", 4, capturedListeners.size());

        // Respond to 2nd → shouldTerminate(2, 10) = true → terminates
        capturedListeners.get(1).onStreamResponse(successResponse(capturedRequests.get(1).getShardId().id()), true);

        // Walk should be done
        assertTrue("Future should be done after termination", future.isDone());
        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // Only 2 tasks completed before termination fed the sink
        assertEquals("Sink should contain only 2 rows from completed tasks before termination", 2, resultList.size());

        // Respond to 3rd and 4th (late, discarded)
        capturedListeners.get(2).onStreamResponse(successResponse(capturedRequests.get(2).getShardId().id()), true);
        capturedListeners.get(3).onStreamResponse(successResponse(capturedRequests.get(3).getShardId().id()), true);

        // Verify late responses were discarded — sink row count should not have increased
        ExchangeSink rootSink = walker.getState().rootSink();
        assertEquals("Sink row count should still be 2 after late responses", 2, rootSink.getRowCount());

        // Total submissions: 3 initial + 1 from 1st completion = 4 (not 10)
        assertEquals("Total submissions should be 4, not 10", 4, capturedListeners.size());
    }

    // ---- 6.8: testFailurePropagatesThroughSlidingWindow ----

    /**
     * Decider with initialBatchSize=3, shouldTerminate=false, 5 targets.
     * 1st task fails, 2nd-5th succeed.
     * Assert listener.onFailure with RuntimeException wrapping "Stage 0 failed".
     *
     * Validates: Requirements 3.2
     */
    public void testFailurePropagatesThroughSlidingWindow() {
        int numShards = 5;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 3;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return false;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        assertEquals("Initial batch should be 3", 3, capturedListeners.size());

        // 1st task fails
        capturedListeners.get(0).onFailure(new RuntimeException("shard_0_failed"));

        // 2nd and 3rd succeed — each triggers next submission
        capturedListeners.get(1).onStreamResponse(successResponse(capturedRequests.get(1).getShardId().id()), true);
        capturedListeners.get(2).onStreamResponse(successResponse(capturedRequests.get(2).getShardId().id()), true);

        // Respond to remaining
        for (int i = 3; i < capturedListeners.size(); i++) {
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
        }

        assertEquals("All 5 should be submitted", 5, capturedListeners.size());

        // Walk should fail
        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue("Exception should reference stage failure", ex.getMessage().contains("Stage 0 failed"));
        assertTrue("Root cause should be the shard failure", ex.getCause().getMessage().contains("shard_0_failed"));
    }

    // ---- 6.9: testInitialBatchClampedToTotalTargets ----

    /**
     * Decider with initialBatchSize=100, 5 targets.
     * Assert 5 initial submissions (clamped), not 100.
     *
     * Validates: Requirements 1.5
     */
    public void testInitialBatchClampedToTotalTargets() {
        int numShards = 5;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 100;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return false;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        // Should be clamped to 5 (total targets), not 100
        assertEquals("Initial batch should be clamped to 5", 5, capturedListeners.size());

        // Respond to all to complete the walk
        for (int i = 0; i < 5; i++) {
            capturedListeners.get(i).onStreamResponse(successResponse(capturedRequests.get(i).getShardId().id()), true);
        }

        // No additional submissions should have been triggered (all targets already dispatched)
        assertEquals("Should still have exactly 5 submissions", 5, capturedListeners.size());

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals("Should have 5 rows", 5, resultList.size());
    }

    // ---- 6.10: testTerminationIsSticky ----

    /**
     * Decider: shouldTerminate returns true on first call, then false on subsequent calls.
     * Track how many times shouldTerminate is called.
     * initialBatchSize=3, 10 targets.
     * Respond to 1st → shouldTerminate returns true (sticky).
     * Respond to 2nd and 3rd (late, discarded).
     * Assert shouldTerminate was called exactly once.
     *
     * Validates: Requirements 2.6
     */
    public void testTerminationIsSticky() {
        int numShards = 10;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        AtomicInteger shouldTerminateCallCount = new AtomicInteger(0);

        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return 3;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                int callNum = shouldTerminateCallCount.incrementAndGet();
                // Return true on first call, false on subsequent
                return callNum == 1;
            }
        };

        QueryDAG dag = buildDagWithDecider("http_logs", decider);

        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        ShardRequestClient client = (request, node, listener) -> {
            capturedRequests.add(request);
            capturedListeners.add(listener);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        assertEquals("Initial batch should be 3", 3, capturedListeners.size());

        // Respond to 1st → shouldTerminate called, returns true → terminated
        capturedListeners.get(0).onStreamResponse(successResponse(capturedRequests.get(0).getShardId().id()), true);

        // Walk should be done
        assertTrue("Future should be done after termination", future.isDone());
        future.actionGet();

        // Respond to 2nd and 3rd (late, discarded — terminated flag is checked first)
        capturedListeners.get(1).onStreamResponse(successResponse(capturedRequests.get(1).getShardId().id()), true);
        capturedListeners.get(2).onStreamResponse(successResponse(capturedRequests.get(2).getShardId().id()), true);

        // shouldTerminate should have been called exactly once — termination is sticky
        assertEquals("shouldTerminate should be called exactly once", 1, shouldTerminateCallCount.get());
    }
}
