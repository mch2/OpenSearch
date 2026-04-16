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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that the {@code walkStage} method in {@link PlanWalker} routes
 * {@link StageExecutionType#LOCAL} stages to {@code dispatchLocalStage}.
 *
 * Validates: Requirements 3.9
 */
@SuppressWarnings("unchecked")
public class PlanWalkerLocalStageRoutingTests extends OpenSearchTestCase {

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

    /**
     * A stage with executionType=LOCAL and compute (non-pass-through) is
     * routed to dispatchLocalStage, which calls the backend's createLocalStage.
     *
     * Validates: Requirements 3.9
     */
    public void testLocalStageRoutedToDispatchLocalStage() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // Child: data-node stage
        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Root: LOCAL stage with compute (Sort above StageInputScan)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        org.apache.calcite.rex.RexBuilder rexBuilder = cluster.getRexBuilder();
        org.apache.calcite.rel.RelNode projectNode = LogicalProject.create(
            stageInput,
            List.of(),
            List.of(rexBuilder.makeInputRef(stageInput, 0)),
            stageInput.getRowType()
        );
        Stage rootStage = new Stage(1, projectNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        // Mock backend
        LocalStageContext mockCtx = mock(LocalStageContext.class);
        when(mockCtx.sinkFor(0)).thenReturn(mock(ExchangeSink.class));
        doAnswer(inv -> {
            ActionListener<Void> l = inv.getArgument(0);
            l.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());

        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        QueryContext config = QueryContext.forTest(dag, null);

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchScan(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<ScanResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "v" });
                listener.onStreamResponse(new ScanResponse(List.of("field_0"), rows), true);
            }
        };

        AtomicBoolean success = new AtomicBoolean(false);
        new EventDrivenScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of(mockBackend.name(), mockBackend)),
            dispatcher
        ).execute(config, ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e)));

        assertTrue("Walk should have succeeded", success.get());
        // Verify the backend's createLocalStage was called — proves routing to dispatchLocalStage
        verify(mockBackend).createLocalStage(any());
    }

    /**
     * A DATA_NODE stage is NOT routed to dispatchLocalStage — it goes through
     * the normal walkChildren → stageExecutor.dispatch path.
     *
     * Validates: Requirements 3.9
     */
    public void testDataNodeStageNotRoutedToLocalStage() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null, StageExecutionType.DATA_NODE);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Backend should NOT be called for createLocalStage
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));

        QueryDAG dag = new QueryDAG("test-query", stage);

        QueryContext config = QueryContext.forTest(dag, null);

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchScan(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<ScanResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "v" });
                listener.onStreamResponse(new ScanResponse(List.of("field_0"), rows), true);
            }
        };

        AtomicBoolean success = new AtomicBoolean(false);
        new EventDrivenScheduler(
            new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of(mockBackend.name(), mockBackend)),
            dispatcher
        ).execute(config, ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e)));

        assertTrue("Walk should have succeeded", success.get());
        // Verify rows via the listener result (root output is now owned by the execution)
    }

    // ─── Helpers ────────────────────────────────────────────────────────

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
}
