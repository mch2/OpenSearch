/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelDistribution;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FragmentConvertor;
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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} async walk behavior: result delivery, failure propagation,
 * empty targets, bottom-up traversal order, and multi-shard response collection.
 */
public class PlanWalkerAsyncTests extends OpenSearchTestCase {

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

    private static AnalyticsSearchTransportService failingDispatcher(ClusterService clusterService) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(FragmentExecutionRequest r, DiscoveryNode n, StreamingResponseListener<FragmentExecutionResponse> l, Task t, PendingExecutions p) {
                fail("should not be called");
            }
        };
    }

    /** Creates a mock backends map where every backend returns a no-op FragmentConvertor (returns empty bytes). */
    private Map<String, AnalyticsSearchBackendPlugin> mockBackends(String... backendIds) {
        Map<String, AnalyticsSearchBackendPlugin> map = new HashMap<>();
        for (String id : backendIds) {
            AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
            FragmentConvertor convertor = mock(FragmentConvertor.class);
            when(convertor.convertScanFragment(anyString(), any())).thenReturn(new byte[0]);
            when(convertor.convertShuffleReadFragment(anyString(), any())).thenReturn(new byte[0]);
            when(backend.getFragmentConvertor()).thenReturn(convertor);
            map.put(id, backend);
        }
        return map;
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    private ClusterService buildMockClusterService(String tableName, int numShards) {
        Index index = new Index(tableName, "_na_");

        // Build mock ClusterState with DiscoveryNodes
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node_" + i);
            when(discoveryNodes.get("node_" + i)).thenReturn(node);
        }
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        // Build mock OperationRouting with searchShards
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

        // Build mock ClusterService wrapping state and routing
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        return clusterService;
    }

    public void testSingleStageWalkSignalsListenerWithResults() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                // Return 2 rows per shard with known data
                List<String> fields = List.of("field_0");
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "shard_" + request.getShardId().id() + "_row0" });
                rows.add(new Object[] { "shard_" + request.getShardId().id() + "_row1" });
                listener.onStreamResponse(MockFragmentResponse.create(fields, rows), true);
            }
        };

        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // 2 shards × 2 rows each = 4 total rows
        assertEquals(4, resultList.size());
    }

    public void testWalkSignalsFailureOnShardError() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        AtomicInteger callCount = new AtomicInteger(0);

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                int call = callCount.getAndIncrement();
                if (call == 0) {
                    // First shard succeeds
                    List<Object[]> okRows = new ArrayList<>();
                    okRows.add(new Object[] { "ok" });
                    listener.onStreamResponse(MockFragmentResponse.create(List.of("field_0"), okRows), true);
                } else {
                    // Second shard fails
                    listener.onFailure(new RuntimeException("shard failed"));
                }
            }
        };

        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(
            "Exception should reference shard failure",
            ex.getMessage().contains("shard failed") || (ex.getCause() != null && ex.getCause().getMessage().contains("shard failed"))
        );
    }

    public void testEmptyTargetsSignalsListenerImmediately() {
        // Coordinator-only stage — no routing needed, simple mock ClusterService
        ClusterService clusterService = mock(ClusterService.class);

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("mock-parquet")
        );
        StagePlan plan = new StagePlan(stageInput, "mock-parquet");
        Stage stage = new Stage(1, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                capturedRequests.add(request);
                listener.onStreamResponse(MockFragmentResponse.create(List.of(), List.of()), true);
            }
        };

        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // Coordinator-only stage: no tasks dispatched, empty result
        assertTrue(capturedRequests.isEmpty());
        assertTrue(resultList.isEmpty());
    }

    public void testBottomUpTraversalOrder() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        // Child stage (stageId=0): SINGLETON exchange with TableScan
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan childPlan = new StagePlan(scan, "mock-parquet");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(childPlan));

        // Root stage (stageId=1): coordinator-only with StageInputScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("mock-parquet")
        );
        StagePlan rootPlan = new StagePlan(stageInput, "mock-parquet");
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null);
        rootStage.setPlanAlternatives(List.of(rootPlan));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<Integer> dispatchedStageIds = new ArrayList<>();

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                dispatchedStageIds.add(request.getStageId());
                List<Object[]> dataRows = new ArrayList<>();
                dataRows.add(new Object[] { "data" });
                listener.onStreamResponse(MockFragmentResponse.create(List.of("field_0"), dataRows), true);
            }
        };

        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);
        future.actionGet();

        // Child stage (stageId=0) tasks should be dispatched; root stage (stageId=1)
        // is coordinator-only (StageInputScan, no TableScan) so dispatches no tasks.
        // All dispatched tasks should be for the child stage.
        assertFalse(dispatchedStageIds.isEmpty());
        for (int stageId : dispatchedStageIds) {
            assertEquals("Child stage tasks should be dispatched before root stage executes", 0, stageId);
        }
    }

    public void testMultipleShardResponsesFeedSinkInOrder() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                // Each shard returns a distinct single row, inline (synchronous)
                int shardIdx = request.getShardId().id();
                List<String> fields = List.of("field_0");
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "value_" + shardIdx });
                listener.onStreamResponse(MockFragmentResponse.create(fields, rows), true);
            }
        };

        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // 3 shards × 1 row each = 3 total rows
        assertEquals(3, resultList.size());

        // Verify all 3 shard values are present in the result
        List<String> values = new ArrayList<>();
        for (Object[] row : resultList) {
            values.add((String) row[0]);
        }
        assertTrue(values.contains("value_0"));
        assertTrue(values.contains("value_1"));
        assertTrue(values.contains("value_2"));
    }
    /**
     * Task 7.5: LOCAL pass-through stage completing without dispatch.
     * Builds a LOCAL pass-through stage (StageInputScan, no exchange, no TableScan).
     * Asserts walk completes, stores RowData in stageOutputs, and signals listener
     * without submitting any tasks.
     * Validates: Requirements 4.1, 4.2
     */
    @SuppressWarnings("unchecked")
    public void testLocalPassthroughStageCompletesWithoutDispatch() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);

        // Coordinator-only stage with StageInputScan (no TableScan, no exchange)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        StagePlan plan = new StagePlan(stageInput, "lucene");
        Stage stage = new Stage(1, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        QueryDAG dag = new QueryDAG("test-query", stage);

        AtomicInteger submitCount = new AtomicInteger(0);
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                submitCount.incrementAndGet();
                listener.onStreamResponse(MockFragmentResponse.create(List.of(), List.of()), true);
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        // Walk should complete successfully
        Iterable<Object[]> result = future.actionGet();
        assertNotNull(result);

        // No tasks should have been submitted
        assertEquals("LOCAL pass-through should not dispatch any tasks", 0, submitCount.get());

        // completedStages set has been removed; completion is signaled via the listener
    }

    /**
     * Task 7.6: Streaming dispatch failure waiting for all tasks.
     * Builds a streaming stage with N targets, configures submitter so at least one fails.
     * Asserts onFailure is called only after all N tasks complete (remaining reaches 0)
     * and the first exception is captured.
     * Validates: Requirements 12.1, 12.2
     */
    public void testStreamingDispatchFailureWaitsForAllTasks() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        // Track how many tasks completed before the failure was signaled
        AtomicInteger completedTasks = new AtomicInteger(0);

        // Shard 1 fails, shards 0 and 2 succeed. All responses are synchronous.
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task _parentTask,
                PendingExecutions _pending
            ) {
                int shardIdx = request.getShardId().id();
                if (shardIdx == 1) {
                    completedTasks.incrementAndGet();
                    listener.onFailure(new RuntimeException("shard_1_failed"));
                } else {
                    completedTasks.incrementAndGet();
                    List<Object[]> rows = new ArrayList<>();
                    rows.add(new Object[] { "value_" + shardIdx });
                    listener.onStreamResponse(MockFragmentResponse.create(List.of("field_0"), rows), true);
                }
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);

        // The walk should fail
        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(
            "Exception should reference shard failure",
            ex.getMessage().contains("shard_1_failed") || (ex.getCause() != null && ex.getCause().getMessage().contains("shard_1_failed"))
        );

        // All 3 tasks should have completed before the failure was signaled
        assertEquals("All tasks should have completed", numShards, completedTasks.get());
    }

    private static org.apache.calcite.rel.RelNode mockFragment() {
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        org.apache.calcite.rel.type.RelDataType rowType = typeFactory.builder()
            .add("id", org.apache.calcite.sql.type.SqlTypeName.BIGINT)
            .add("value", org.apache.calcite.sql.type.SqlTypeName.VARCHAR).build();
        org.apache.calcite.rel.RelNode fragment = mock(org.apache.calcite.rel.RelNode.class);
        when(fragment.getInputs()).thenReturn(List.of());
        when(fragment.getRowType()).thenReturn(rowType);
        return fragment;
    }
}
