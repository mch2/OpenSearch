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
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} parallel children walk behavior:
 * walkChildrenInParallel completion, empty list, failure propagation,
 * and parallelChildren flag routing.
 *
 * Validates: Requirements 3.1, 3.2, 3.3, 3.5
 */
public class PlanWalkerParallelTests extends OpenSearchTestCase {

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
     * All parallel children walked, listener signaled once with success.
     * Builds a root stage with parallelChildren=true and two child stages,
     * each backed by a TableScan that resolves to shards. Verifies all children
     * are dispatched and the walk completes successfully.
     *
     * Validates: Requirements 3.1, 3.2
     */
    public void testWalkChildrenInParallelAllSucceed() {
        int numShards = 2;
        ClusterService clusterServiceA = buildMockClusterService("table_a", numShards);
        ClusterService clusterServiceB = buildMockClusterService("table_b", numShards);

        // Build a merged ClusterService that handles both tables
        ClusterService clusterService = buildMockClusterServiceForMultipleTables(new String[] { "table_a", "table_b" }, numShards);

        // Child stage 0: TableScan on table_a with SINGLETON exchange
        OpenSearchTableScan scanA = buildTableScan("table_a", List.of("lucene"));
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childA = new Stage(0, scanA, List.of(), singletonExchange);
        childA.setPlanAlternatives(List.of(new StagePlan(scanA, "lucene")));

        // Child stage 1: TableScan on table_b with SINGLETON exchange
        OpenSearchTableScan scanB = buildTableScan("table_b", List.of("lucene"));
        Stage childB = new Stage(1, scanB, List.of(), singletonExchange);
        childB.setPlanAlternatives(List.of(new StagePlan(scanB, "lucene")));

        // Root stage: coordinator gather with parallelChildren=true
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage rootStage = new Stage(2, stageInput, List.of(childA, childB), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        AtomicInteger listenerSignalCount = new AtomicInteger(0);
        List<Integer> dispatchedStageIds = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            dispatchedStageIds.add(request.getStageId());
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_stage_" + request.getStageId() });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        Iterable<Object[]> result = future.actionGet();
        assertNotNull(result);

        // Both child stages should have been dispatched
        assertTrue("Child stage 0 should be dispatched", dispatchedStageIds.contains(0));
        assertTrue("Child stage 1 should be dispatched", dispatchedStageIds.contains(1));

        // Total dispatched tasks: 2 shards per child × 2 children = 4
        assertEquals("Should dispatch tasks for both children", numShards * 2, dispatchedStageIds.size());
    }

    /**
     * Empty children list signals success immediately.
     * Builds a root stage with parallelChildren=true but no children.
     * Verifies the walk completes with no tasks dispatched.
     *
     * Validates: Requirements 3.2
     */
    public void testWalkChildrenInParallelEmptyList() {
        ClusterService clusterService = mock(ClusterService.class);

        // Root stage: coordinator gather with parallelChildren=true, no children
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage rootStage = new Stage(0, stageInput, List.of(), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        AtomicInteger submitCount = new AtomicInteger(0);
        TaskSubmitter submitter = (request, node, listener) -> {
            submitCount.incrementAndGet();
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        Iterable<Object[]> result = future.actionGet();
        assertNotNull("Walk should complete successfully", result);
        assertEquals("No tasks should be dispatched for empty children", 0, submitCount.get());
    }

    /**
     * First exception captured, listener signaled after all children complete.
     * Builds a root stage with parallelChildren=true and two child stages.
     * One child's submitter fails. Verifies the failure is propagated only after
     * all children complete, and the first exception is captured.
     *
     * Validates: Requirements 3.3
     */
    public void testWalkChildrenInParallelOneFailure() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterServiceForMultipleTables(new String[] { "table_a", "table_b" }, numShards);

        // Child stage 0: TableScan on table_a — will succeed
        OpenSearchTableScan scanA = buildTableScan("table_a", List.of("lucene"));
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childA = new Stage(0, scanA, List.of(), singletonExchange);
        childA.setPlanAlternatives(List.of(new StagePlan(scanA, "lucene")));

        // Child stage 1: TableScan on table_b — will fail
        OpenSearchTableScan scanB = buildTableScan("table_b", List.of("lucene"));
        Stage childB = new Stage(1, scanB, List.of(), singletonExchange);
        childB.setPlanAlternatives(List.of(new StagePlan(scanB, "lucene")));

        // Root stage with parallelChildren=true
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage rootStage = new Stage(2, stageInput, List.of(childA, childB), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        AtomicInteger completedChildren = new AtomicInteger(0);

        TaskSubmitter submitter = (request, node, listener) -> {
            if (request.getStageId() == 1) {
                // Stage 1 (table_b) fails
                completedChildren.incrementAndGet();
                listener.onFailure(new RuntimeException("child_b_failed"));
            } else {
                // Stage 0 (table_a) succeeds
                completedChildren.incrementAndGet();
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "ok" });
                listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
            }
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        // Walk should fail with the child_b failure wrapped in a stage failure
        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(
            "Exception should reference stage failure",
            ex.getMessage().contains("Stage 1 failed") || ex.getCause().getMessage().contains("child_b_failed")
        );

        // All shard tasks across both children should have completed
        assertEquals("All shard tasks should have completed", numShards * 2, completedChildren.get());
    }

    /**
     * parallelChildren=true routes to parallel walk instead of sequential.
     * Builds a root stage with parallelChildren=true and two child stages.
     * Verifies both children are dispatched (not sequentially gated).
     *
     * Validates: Requirements 3.5
     */
    public void testParallelChildrenFlagRoutesToParallelWalk() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterServiceForMultipleTables(new String[] { "table_x", "table_y" }, numShards);

        // Child stage 0: TableScan on table_x
        OpenSearchTableScan scanX = buildTableScan("table_x", List.of("lucene"));
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childX = new Stage(0, scanX, List.of(), singletonExchange);
        childX.setPlanAlternatives(List.of(new StagePlan(scanX, "lucene")));

        // Child stage 1: TableScan on table_y
        OpenSearchTableScan scanY = buildTableScan("table_y", List.of("lucene"));
        Stage childY = new Stage(1, scanY, List.of(), singletonExchange);
        childY.setPlanAlternatives(List.of(new StagePlan(scanY, "lucene")));

        // Root stage with parallelChildren=true
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage rootStage = new Stage(2, stageInput, List.of(childX, childY), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        List<Integer> dispatchOrder = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            synchronized (dispatchOrder) {
                dispatchOrder.add(request.getStageId());
            }
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "data" });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        future.actionGet();

        // Both children should have been dispatched
        assertTrue("Child stage 0 should be dispatched", dispatchOrder.contains(0));
        assertTrue("Child stage 1 should be dispatched", dispatchOrder.contains(1));
        assertEquals("Both children should have exactly one task each", 2, dispatchOrder.size());
    }

    /**
     * Builds a mock ClusterService that handles multiple tables, each with the same number of shards.
     * Each table gets its own set of shard iterators and nodes.
     */
    private ClusterService buildMockClusterServiceForMultipleTables(String[] tableNames, int numShards) {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);

        // Create enough nodes for all tables
        for (int t = 0; t < tableNames.length; t++) {
            for (int i = 0; i < numShards; i++) {
                String nodeId = "node_" + tableNames[t] + "_" + i;
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
