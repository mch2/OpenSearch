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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link StageExecutor} primary backend injection (Task 9).
 * Validates: Requirements 2.5, 3.1, 3.3
 */
@SuppressWarnings("unchecked")
public class StageExecutorPrimaryBackendTests extends OpenSearchTestCase {

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
     * Construct StageExecutor with a mock backend, dispatch a compute LOCAL
     * stage, and verify the backend's createLocalStage is called and
     * asyncFinalize fires on success.
     *
     * Validates: Requirements 3.1
     */
    public void testPrimaryBackendInjection() {
        ClusterService clusterService = buildMockClusterService("test_table", 1);

        // Child: data-node stage
        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo exchange = new ExchangeInfo(SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), exchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Root: compute LOCAL stage (non-pass-through)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, sortNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1, 2, 3 })));

        // Mock backend
        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink childSink = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(childSink);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());

        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        // 2-arg constructor with backend
        StageExecutor executor = new StageExecutor(clusterService, mockBackend);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row" });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();

        // Use a ChildDispatcher that handles the data-node child via the executor itself
        ChildDispatcher childDispatcher = (s, sink, c, l) -> executor.dispatch(
            s,
            sink,
            c,
            (s2, sink2, c2, l2) -> l2.onResponse(null),
            config,
            state,
            l
        );

        executor.dispatch(
            rootStage,
            state.rootSink(),
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("Dispatch should have succeeded", success.get());
        assertNull("No error expected", error.get());
        verify(mockBackend).createLocalStage(any());
        verify(mockCtx).sinkFor(0);
        verify(mockCtx).asyncFinalize(any());
    }

    /**
     * Construct StageExecutor with null primaryBackend (test-only 1-arg ctor),
     * dispatch a DATA_NODE stage, and verify it still works.
     *
     * Validates: Requirements 2.5
     */
    public void testNoPrimaryBackendAllowedForDataNodeOnlyUse() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // 1-arg test-only constructor (null backend)
        StageExecutor executor = new StageExecutor(clusterService);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();

        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);
        executor.dispatch(
            stage,
            state.rootSink(),
            client,
            noOpChildren,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("DATA_NODE dispatch should succeed with null backend", success.get());
        assertNull("No error expected", error.get());
    }

    /**
     * Construct StageExecutor with null primaryBackend (test-only 1-arg ctor),
     * dispatch a compute LOCAL stage, and verify it fails fast with a clear
     * IllegalStateException mentioning "primaryBackend".
     *
     * Validates: Requirements 3.3
     */
    public void testNullPrimaryBackendFailsFastOnComputeLocalStage() {
        ClusterService clusterService = mock(ClusterService.class);

        // 1-arg test-only constructor (null backend)
        StageExecutor executor = new StageExecutor(clusterService);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();

        // Compute LOCAL stage (non-pass-through)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, sortNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1 })));

        AtomicReference<Exception> captured = new AtomicReference<>();
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);
        executor.dispatch(
            stage,
            state.rootSink(),
            (request, node, listener) -> fail("should not be called"),
            noOpChildren,
            config,
            state,
            ActionListener.wrap(v -> fail("should not succeed"), captured::set)
        );

        Exception e = captured.get();
        assertNotNull("Should have received failure", e);
        assertTrue("Should be IllegalStateException, got: " + e.getClass().getName(), e instanceof IllegalStateException);
        assertTrue(
            "Message should mention primaryBackend, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("primaryBackend")
        );
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, List.of("lucene"), List.of());
    }

    private RelNode buildNonPassthroughFragment(RelNode input) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        return org.apache.calcite.rel.logical.LogicalProject.create(
            input,
            List.of(),
            List.of(rexBuilder.makeInputRef(input, 0)),
            input.getRowType()
        );
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
