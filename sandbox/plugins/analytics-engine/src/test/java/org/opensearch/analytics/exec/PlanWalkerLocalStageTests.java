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
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.backend.LocalStageContext;
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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker}'s {@code dispatchLocalStage} method.
 * Covers single child, multi-child, child failure, finalize-on-success,
 * cancel-on-child-failure, listener exactly-once, and pass-through bypass.
 *
 * Validates: Requirements 3.9
 */
@SuppressWarnings("unchecked")
public class PlanWalkerLocalStageTests extends OpenSearchTestCase {

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
     * A LOCAL stage with a single data-node child: the walker obtains a
     * LocalStageContext from the backend, walks the child with the per-child
     * sink, and calls asyncFinalize on success.
     *
     * Validates: Requirements 3.9
     */
    public void testSingleChildLocalStage() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // Child stage (stageId=0): data-node stage with TableScan and SINGLETON exchange
        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Root stage (stageId=1): LOCAL with StageInputScan + compute (not pass-through)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        // Wrap in a trivial parent to make it non-pass-through
        org.apache.calcite.rel.RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, sortNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1, 2, 3 })));

        // Mock backend and capability registry
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
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        QueryState state = new QueryState();
        QueryContext config = QueryContext.forTest(dag, null);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row" });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService, mockBackend));

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        walker.walk(client, ActionListener.wrap(v -> success.set(true), error::set));

        assertTrue("Walk should have succeeded", success.get());
        assertNull("No error expected", error.get());
        verify(mockCtx).asyncFinalize(any());
        verify(mockCtx).sinkFor(0);
    }

    /**
     * A LOCAL stage with two data-node children: both children complete
     * successfully, then asyncFinalize is called.
     *
     * Validates: Requirements 3.9
     */
    public void testMultiChildLocalStage() {
        ClusterService clusterService = buildMockClusterServiceForMultipleTables(new String[] { "table_a", "table_b" }, 1);

        // Child stage 0: data-node on table_a
        OpenSearchTableScan scanA = buildTableScan("table_a");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childA = new Stage(0, scanA, List.of(), singletonExchange);
        childA.setPlanAlternatives(List.of(new StagePlan(scanA, "lucene")));

        // Child stage 1: data-node on table_b
        OpenSearchTableScan scanB = buildTableScan("table_b");
        Stage childB = new Stage(1, scanB, List.of(), singletonExchange);
        childB.setPlanAlternatives(List.of(new StagePlan(scanB, "lucene")));

        // Root stage: LOCAL with compute
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        org.apache.calcite.rel.RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(2, sortNode, List.of(childA, childB), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1 })));

        // Mock backend
        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink sinkA = mock(ExchangeSink.class);
        ExchangeSink sinkB = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(sinkA);
        when(mockCtx.sinkFor(1)).thenReturn(sinkB);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());

        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        QueryState state = new QueryState();
        QueryContext config = QueryContext.forTest(dag, null);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getStageId() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService, mockBackend));

        AtomicBoolean success = new AtomicBoolean(false);
        walker.walk(client, ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e)));

        assertTrue("Walk should have succeeded", success.get());
        verify(mockCtx).sinkFor(0);
        verify(mockCtx).sinkFor(1);
        verify(mockCtx).asyncFinalize(any());
    }

    /**
     * When a child stage fails, the walker calls failChildStage on the
     * LocalStageExecution, which closes the context and signals the listener.
     *
     * Validates: Requirements 3.9
     */
    public void testChildFailureClosesContext() {
        ClusterService clusterService = buildMockClusterService("test_table", 1);

        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        org.apache.calcite.rel.RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, sortNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1 })));

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink childSink = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(childSink);

        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        QueryState state = new QueryState();
        QueryContext config = QueryContext.forTest(dag, null);

        // Client that fails
        RuntimeException childError = new RuntimeException("shard exploded");
        ShardRequestClient client = (request, node, listener) -> listener.onFailure(childError);

        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService, mockBackend));

        AtomicReference<Exception> captured = new AtomicReference<>();
        walker.walk(client, ActionListener.wrap(v -> fail("should not succeed"), captured::set));

        assertNotNull("Should have received failure", captured.get());
        verify(mockCtx).close();
        verify(mockCtx, never()).asyncFinalize(any());
    }

    /**
     * A pass-through LOCAL stage (bare OpenSearchStageInputScan, no compute)
     * bypasses the backend entirely. No createLocalStage call, no sinkFor call.
     * Child output feeds straight through to the parent's outputSink.
     *
     * Validates: Requirements 3.9
     */
    public void testPassthroughLocalStageBypassesBackend() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // Child stage (stageId=0): data-node stage
        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Root stage (stageId=1): pass-through LOCAL (bare StageInputScan)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("lucene")
        );
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        // Backend should NOT be called
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);
        QueryContext config = QueryContext.forTest(dag, null);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService, mockBackend));

        AtomicBoolean success = new AtomicBoolean(false);
        walker.walk(client, ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e)));

        assertTrue("Walk should have succeeded", success.get());
        // Backend should NOT have been called
        verify(mockBackend, never()).createLocalStage(any());
        // Rows should have reached the rootSink directly
        assertEquals("rootSink should have rows from all shards", numShards, rootSink.getRowCount());
    }

    /**
     * Listener is signaled exactly once even when asyncFinalize succeeds.
     *
     * Validates: Requirements 3.9
     */
    public void testListenerSignaledExactlyOnce() {
        int numShards = 1;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        org.apache.calcite.rel.RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, sortNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1 })));

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        when(mockCtx.sinkFor(0)).thenReturn(mock(ExchangeSink.class));
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());

        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");
        when(mockBackend.supportedOperators()).thenReturn(Set.of(OperatorCapability.LOCAL_STAGE));
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        QueryState state = new QueryState();
        QueryContext config = QueryContext.forTest(dag, null);

        ShardRequestClient client = (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "v" });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService, mockBackend));

        java.util.concurrent.atomic.AtomicInteger responseCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger failureCount = new java.util.concurrent.atomic.AtomicInteger(0);
        walker.walk(client, new ActionListener<>() {
            @Override
            public void onResponse(Iterable<Object[]> v) {
                responseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
            }
        });

        assertEquals("Listener should be called exactly once (success)", 1, responseCount.get());
        assertEquals("No failures expected", 0, failureCount.get());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, List.of("lucene"), List.of());
    }

    /**
     * Builds a trivial Project node above the given input to make the fragment
     * non-pass-through (i.e., not a bare OpenSearchStageInputScan).
     */
    private org.apache.calcite.rel.RelNode buildNonPassthroughFragment(org.apache.calcite.rel.RelNode input) {
        org.apache.calcite.rex.RexBuilder rexBuilder = input.getCluster().getRexBuilder();
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

    private ClusterService buildMockClusterServiceForMultipleTables(String[] tableNames, int numShards) {
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);

        for (String tableName : tableNames) {
            for (int i = 0; i < numShards; i++) {
                String nodeId = "node_" + tableName + "_" + i;
                DiscoveryNode node = mock(DiscoveryNode.class);
                when(node.getId()).thenReturn(nodeId);
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

    /**
     * When the test-only 1-arg {@link StageExecutor} constructor is used (null
     * primaryBackend) and the walker is asked to dispatch a compute LOCAL
     * stage, it must fail fast with a clear {@link IllegalStateException}
     * instead of NPE. Pass-through LOCAL stages are unaffected (they don't
     * touch the backend).
     */
    public void testLegacyConstructorFailsFastOnComputeLocalStage() {
        ClusterService clusterService = buildMockClusterService("test_table", 1);

        OpenSearchTableScan scan = buildTableScan("test_table");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        org.apache.calcite.rel.RelNode sortNode = buildNonPassthroughFragment(stageInput);
        Stage rootStage = new Stage(1, sortNode, List.of(childStage), null, StageExecutionType.LOCAL);
        rootStage.setPlanAlternatives(List.of(new StagePlan(sortNode, "test-backend", new byte[] { 1 })));

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        QueryState state = new QueryState();
        QueryContext config = QueryContext.forTest(dag, null);

        ShardRequestClient client = (request, node, listener) -> {
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), List.of()), true);
        };

        // Test-only 1-arg constructor — no primaryBackend
        PlanWalker walker = new PlanWalker(config, state, new StageExecutor(clusterService));

        AtomicReference<Exception> captured = new AtomicReference<>();
        walker.walk(client, ActionListener.wrap(v -> fail("should not succeed"), captured::set));

        Exception e = captured.get();
        assertNotNull("Should have received failure", e);
        assertTrue("Should be IllegalStateException, got: " + e.getClass().getName(), e instanceof IllegalStateException);
        assertTrue(
            "Message should mention primaryBackend, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("primaryBackend")
        );
    }
}
