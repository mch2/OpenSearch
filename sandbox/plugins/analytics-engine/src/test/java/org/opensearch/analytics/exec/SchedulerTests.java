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
import org.opensearch.analytics.backend.FragmentExecutionResponse;
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
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Scheduler}: walker pool lifecycle (success/failure) and
 * dispatch via {@link ShardTransportDispatcher}.
 */
@SuppressWarnings("unchecked")
public class SchedulerTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.get(any())).thenReturn(mock(DiscoveryNode.class));
        when(state.nodes()).thenReturn(nodes);
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

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

    private ConcurrentMap<String, PlanWalker> getWalkerPool(Scheduler scheduler) throws Exception {
        Field walkerPoolField = Scheduler.class.getDeclaredField("walkerPool");
        walkerPoolField.setAccessible(true);
        return (ConcurrentMap<String, PlanWalker>) walkerPoolField.get(scheduler);
    }

    public void testWalkerRemovedFromPoolAfterSuccess() throws Exception {
        StreamTransportService transportService = mock(StreamTransportService.class);
        BiFunction<String, String, Transport.Connection> connectionLookup = (alias, nodeId) -> mock(Transport.Connection.class);
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, clusterService, 5);
        Scheduler scheduler = new Scheduler(dispatcher, new StageExecutor(clusterService));

        // Build a coordinator-only stage (StageInputScan, no TableScan) → empty targets → immediate success
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
        QueryDAG dag = new QueryDAG("test-query-success", stage);

        // Coordinator-only stage — no routing needed, simple mock ClusterService
        ClusterService clusterService = mock(ClusterService.class);
        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(clusterService));

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);

        // Future should complete successfully with empty result
        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertTrue(resultList.isEmpty());

        // Walker should be removed from pool after success
        ConcurrentMap<String, PlanWalker> pool = getWalkerPool(scheduler);
        assertTrue(pool.isEmpty());
    }

    public void testWalkerRemovedFromPoolOnFailure() throws Exception {
        StreamTransportService transportService = mock(StreamTransportService.class);
        // Build a single-stage DAG with 1 shard so that dispatchTask is called.
        ClusterService routingCs = buildMockClusterService("http_logs", 1);
        when(transportService.getConnection(any(DiscoveryNode.class))).thenReturn(mock(Transport.Connection.class));

        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, routingCs, 5);
        Scheduler scheduler = new Scheduler(dispatcher, new StageExecutor(routingCs));

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query-failure", stage);

        // Mock transportService.sendChildRequest to call onFailure on the response handler
        doAnswer(invocation -> {
            org.opensearch.transport.TransportResponseHandler<?> handler = invocation.getArgument(5);
            handler.handleException(new org.opensearch.transport.TransportException("shard execution failed"));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                nullable(org.opensearch.tasks.Task.class),
                any(org.opensearch.transport.TransportRequestOptions.class),
                any(org.opensearch.transport.TransportResponseHandler.class)
            );

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(routingCs));

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);

        // Future should complete with failure
        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(ex.getMessage().contains("Stage 0 failed") || ex.getCause().getMessage().contains("shard execution failed"));

        // Walker should be removed from pool after failure
        ConcurrentMap<String, PlanWalker> pool = getWalkerPool(scheduler);
        assertTrue(pool.isEmpty());
    }

    public void testDispatchShardRequestCallsTransportServiceSendRequest() throws Exception {
        StreamTransportService transportService = mock(StreamTransportService.class);
        when(transportService.getConnection(any(DiscoveryNode.class))).thenReturn(mock(Transport.Connection.class));

        // Build single-stage DAG with 1 shard
        ClusterService routingCs = buildMockClusterService("http_logs", 1);

        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, routingCs, 5);
        Scheduler scheduler = new Scheduler(dispatcher, new StageExecutor(routingCs));

        // Mock transportService.sendChildRequest to call onResponse immediately via handler
        doAnswer(invocation -> {
            org.opensearch.transport.TransportResponseHandler<FragmentExecutionResponse> handler = invocation.getArgument(5);
            handler.handleResponse(new FragmentExecutionResponse(List.of("field_0"), List.of()));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                nullable(org.opensearch.tasks.Task.class),
                any(org.opensearch.transport.TransportRequestOptions.class),
                any(org.opensearch.transport.TransportResponseHandler.class)
            );

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query-dispatch", stage);

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(routingCs));

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, future);

        // Wait for completion
        future.actionGet();

        // Verify transportService.sendChildRequest was called with the shard action name
        verify(transportService).sendChildRequest(
            any(Transport.Connection.class),
            eq(AnalyticsShardAction.NAME),
            any(TransportRequest.class),
            nullable(org.opensearch.tasks.Task.class),
            any(org.opensearch.transport.TransportRequestOptions.class),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );
    }
}
