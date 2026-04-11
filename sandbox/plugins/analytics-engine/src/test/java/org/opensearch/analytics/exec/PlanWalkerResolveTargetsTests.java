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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} target resolution via walk() with a capturing TaskSubmitter.
 */
public class PlanWalkerResolveTargetsTests extends OpenSearchTestCase {

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

    public void testLeafStageWithTableScanResolvesAllPrimaryShards() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);
        future.actionGet();

        assertEquals(numShards, capturedRequests.size());
        for (int i = 0; i < numShards; i++) {
            ShardId shardId = capturedRequests.get(i).getShardId();
            assertEquals("http_logs", shardId.getIndexName());
            assertEquals(i, shardId.id());
        }
    }

    public void testSingletonExchangeResolvesAllPrimaryShards() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(plan));

        // Root stage with StageInputScan (coordinator-only)
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
        List<FragmentExecutionRequest> capturedRequests = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);
        future.actionGet();

        // Only the child stage (SINGLETON with TableScan) should dispatch tasks.
        // The root stage (coordinator-only with StageInputScan) resolves empty targets.
        assertEquals(numShards, capturedRequests.size());
        for (int i = 0; i < numShards; i++) {
            ShardId shardId = capturedRequests.get(i).getShardId();
            assertEquals("http_logs", shardId.getIndexName());
            assertEquals(i, shardId.id());
        }
    }

    public void testCoordinatorOnlyStageResolvesEmpty() {
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

        TaskSubmitter submitter = (request, node, listener) -> {
            capturedRequests.add(request);
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);
        future.actionGet();

        // Coordinator-only stage with StageInputScan (no TableScan) should dispatch zero tasks
        assertTrue(capturedRequests.isEmpty());
    }
}
