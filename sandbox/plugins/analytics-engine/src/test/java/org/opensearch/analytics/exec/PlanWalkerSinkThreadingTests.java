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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the sink-aware {@code walkStage} path in {@link PlanWalker}
 * threads the {@code outputSink} parameter through {@code walkChildren}
 * into the recursive {@code walkStage} calls and ultimately to the
 * {@link StageExecutor#dispatch} call, rather than always using
 * {@code state.rootSink()}.
 *
 * Validates: Requirements 3.1, 3.2, 3.3
 */
public class PlanWalkerSinkThreadingTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, List.of("lucene"), List.of());
    }

    private ClusterService buildMockClusterService(String tableName, int numShards) {
        Index index = new Index(tableName, "_na_");
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
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
     * Walks a two-stage DAG (child data-node stage + root coordinator-gather stage)
     * via {@link PlanWalker#walk}. The walk entry point passes {@code state.rootSink()}
     * as the initial output sink. Verifies that the child stage's dispatch receives
     * the threaded sink (which is rootSink in this case) and rows end up there.
     *
     * This confirms that {@code dispatchStage} threads the outputSink through
     * the recursive call and ultimately to
     * {@code stageExecutor.dispatch(stage, sink, client, childDispatcher, config, state, listener)}.
     *
     * Validates: Requirements 3.1, 3.2, 3.3
     */
    public void testWalkStageThreadsOutputSinkToDispatch() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // Child stage (stageId=0): data-node stage with TableScan and SINGLETON exchange
        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Root stage (stageId=1): coordinator-gather with StageInputScan (no exchange, no TableScan)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("lucene")
        );
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        // Create state with a known rootSink so we can inspect it
        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);

        // Client returns one row per shard
        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), state, new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // walk() passes state.rootSink() as the initial outputSink.
        // PlanWalker.dispatchStage threads it through StageExecutor.dispatch into the child's
        // recursive dispatchStage call, which passes it to stageExecutor.dispatch(stage, sink, client, ...).
        // The dispatch uses the sink to construct SinkFeedingHandler, so rows land in rootSink.
        assertEquals("rootSink should have received rows from all shards", numShards, rootSink.getRowCount());
        assertEquals("result should contain rows from all shards", numShards, resultList.size());
    }

    /**
     * Walks a single data-node stage (no children, no coordinator-gather parent).
     * Verifies that the outputSink threaded from walk() reaches the dispatch call
     * and rows end up in the provided sink, not a different one.
     *
     * Validates: Requirements 3.1, 3.2
     */
    public void testSingleStageThreadsSinkToDispatch() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", stage);

        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "value_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), state, new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        future.actionGet();

        // All rows should have been fed into rootSink via the threaded outputSink path
        assertEquals("rootSink should have received rows from all shards", numShards, rootSink.getRowCount());
    }

    /**
     * Walks a DAG with two parallel child stages under a coordinator-gather root.
     * Verifies that the outputSink is threaded through walkChildren to both
     * children's walkStage calls and both children's rows end up in the same sink.
     *
     * Validates: Requirements 3.2, 3.3
     */
    public void testParallelChildrenThreadSameSink() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterServiceForMultipleTables(new String[] { "table_a", "table_b" }, numShards);

        // Child stage 0: data-node stage on table_a
        OpenSearchTableScan scanA = buildTableScan("table_a");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childA = new Stage(0, scanA, List.of(), singletonExchange);
        childA.setPlanAlternatives(List.of(new StagePlan(scanA, "lucene")));

        // Child stage 1: data-node stage on table_b
        OpenSearchTableScan scanB = buildTableScan("table_b");
        Stage childB = new Stage(1, scanB, List.of(), singletonExchange);
        childB.setPlanAlternatives(List.of(new StagePlan(scanB, "lucene")));

        // Root stage: coordinator-gather
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("lucene")
        );
        Stage rootStage = new Stage(2, stageInput, List.of(childA, childB), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "stage_" + request.getStageId() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), state, new StageExecutor(clusterService));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);

        future.actionGet();

        // Both children should have fed their rows into the same rootSink
        // 1 shard per child × 2 children = 2 rows total
        assertEquals("rootSink should have received rows from both children", 2, rootSink.getRowCount());
    }

    private ClusterService buildMockClusterServiceForMultipleTables(String[] tableNames, int numShards) {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);

        for (String tableName : tableNames) {
            for (int i = 0; i < numShards; i++) {
                String nodeId = "node_" + tableName + "_" + i;
                DiscoveryNode node = mock(DiscoveryNode.class);
                when(discoveryNodes.get(nodeId)).thenReturn(node);
            }
        }
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        OperationRouting operationRouting = mock(OperationRouting.class);

        for (String tableName : tableNames) {
            Index index = new Index(tableName, "_na_");
            List<ShardIterator> iterators = new ArrayList<>();
            for (int i = 0; i < numShards; i++) {
                ShardIterator shardIt = mock(ShardIterator.class);
                ShardRouting shard = mock(ShardRouting.class);
                when(shard.shardId()).thenReturn(new ShardId(index, i));
                when(shard.currentNodeId()).thenReturn("node_" + tableName + "_" + i);
                when(shardIt.nextOrNull()).thenReturn(shard);
                iterators.add(shardIt);
            }
            GroupShardsIterator<ShardIterator> groupIterator = new GroupShardsIterator<>(iterators);
            when(operationRouting.searchShards(any(), eq(new String[] { tableName }), isNull(), isNull())).thenReturn(groupIterator);
        }

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);
        return clusterService;
    }
}
