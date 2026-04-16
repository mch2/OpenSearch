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
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.ShuffleImpl;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Focused unit tests for {@link ShardFanOutStageScheduler} — the scheduler
 * extracted from {@code StageExecutor.dispatchDataNodeStage}.
 *
 * Validates: Requirements 3.3
 */
@SuppressWarnings("unchecked")
public class ShardFanOutStageSchedulerTests extends OpenSearchTestCase {

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

    // ─── Helpers ────────────────────────────────────────────────────────

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
     * Creates a ShardRequestClient that responds immediately with row data.
     */
    private ShardRequestClient immediateRowClient() {
        return (request, node, listener) -> {
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };
    }

    /**
     * Creates a ShardRequestClient that responds immediately with metadata (for shuffle-write).
     */
    private ShardRequestClient immediateMetadataClient() {
        return (request, node, listener) -> {
            Map<String, String> metadata = Map.of("0", "/path/to/partition_0_shard_" + request.getShardId().id());
            listener.onStreamResponse(new FragmentExecutionResponse(metadata), true);
        };
    }

    // ─── Task 12: testDispatchesDataNodeStage ───────────────────────────

    /**
     * DATA_NODE stage with mock targets (2 shards). Assert that
     * {@link FanOutStageExecution} is constructed and {@code run()} called
     * (verified via {@code state.registerStageExecution} being called, and
     * the listener completing successfully).
     *
     * Validates: Requirements 3.3
     */
    public void testDispatchesDataNodeStage() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = immediateRowClient();
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(stage, outputSink, client, noOpChildren, config, state, ActionListener.wrap(v -> success.set(true), error::set));

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Stage should be in completedStages", state.completedStages().contains(0));
    }

    // ─── Task 13: testShuffleWriteUsesManifestCollectingHandler ─────────

    /**
     * {@code stage.isShuffleWrite() == true} → verify the handler is
     * {@link ManifestCollectingHandler}. Create a stage with
     * {@link ExchangeInfo} that has a shuffle. Verify the stage completes
     * and manifests are collected in {@code state.shuffleManifests()}.
     *
     * Validates: Requirements 3.3
     */
    public void testShuffleWriteUsesManifestCollectingHandler() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        // Build a shuffle-write stage: ExchangeInfo with HASH_DISTRIBUTED + ShuffleImpl
        ExchangeInfo shuffleExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), shuffleExchange);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        assertTrue("Stage should be shuffle-write", stage.isShuffleWrite());

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = immediateMetadataClient();
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(stage, outputSink, client, noOpChildren, config, state, ActionListener.wrap(v -> success.set(true), error::set));

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Stage should be in completedStages", state.completedStages().contains(0));
        // Manifests should have been collected via ManifestCollectingHandler
        assertTrue("shuffleManifests should contain stage 0", state.shuffleManifests().containsKey(0));
        assertEquals("Should have manifests from 2 shards", numShards, state.shuffleManifests().get(0).size());
    }

    // ─── Task 14: testNonShuffleWriteUsesSinkFeedingHandler ─────────────

    /**
     * {@code stage.isShuffleWrite() == false} → verify the handler feeds
     * the outputSink. Create a normal DATA_NODE stage with no shuffle.
     * Verify rows are fed to the outputSink (check
     * {@code rootSink.getRowCount() > 0}).
     *
     * Validates: Requirements 3.3
     */
    public void testNonShuffleWriteUsesSinkFeedingHandler() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        assertFalse("Stage should NOT be shuffle-write", stage.isShuffleWrite());

        QueryContext config = QueryContext.forTest("test-query", null);
        SimpleExchangeSink outputSink = new SimpleExchangeSink();
        QueryState state = new QueryState(outputSink);
        ShardRequestClient client = immediateRowClient();
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(stage, outputSink, client, noOpChildren, config, state, ActionListener.wrap(v -> success.set(true), error::set));

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Rows should have been fed to the outputSink", outputSink.getRowCount() > 0);
        assertFalse("shuffleManifests should NOT contain stage 0", state.shuffleManifests().containsKey(0));
    }

    // ─── Task 15: testWalksChildrenBeforeFanOut ─────────────────────────

    /**
     * Stage with children → children walked first via
     * {@link StageSchedulerHelpers#walkChildrenWithSink}, fan-out happens
     * after {@code onResponse}. Use an {@link AtomicBoolean} to track
     * ordering: child dispatch happens before shard requests.
     *
     * Validates: Requirements 3.3
     */
    public void testWalksChildrenBeforeFanOut() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        // Parent DATA_NODE stage with one child
        OpenSearchStageInputScan childFragment = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            99,
            rowType,
            List.of("lucene")
        );
        Stage child = new Stage(1, childFragment, List.of(), null, StageExecutionType.LOCAL);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(child), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();

        AtomicBoolean childDispatchedFirst = new AtomicBoolean(false);
        AtomicBoolean shardRequestSent = new AtomicBoolean(false);

        ShardRequestClient client = (request, node, listener) -> {
            // By the time shard requests fire, child should already be dispatched
            assertTrue("Child should have been dispatched before shard requests", childDispatchedFirst.get());
            shardRequestSent.set(true);
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        ChildDispatcher childDispatcher = (s, sink, c, l) -> {
            assertFalse("Shard requests should NOT have been sent yet", shardRequestSent.get());
            childDispatchedFirst.set(true);
            l.onResponse(null);
        };

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Child should have been dispatched", childDispatchedFirst.get());
        assertTrue("Shard requests should have been sent", shardRequestSent.get());
    }

    // ─── Task 16: testChildFailurePropagates ────────────────────────────

    /**
     * Child dispatch fails → fan-out is NOT called; listener receives
     * {@code onFailure}. Verify no shard requests are made.
     *
     * Validates: Requirements 3.3
     */
    public void testChildFailurePropagates() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        // Parent DATA_NODE stage with one child
        OpenSearchStageInputScan childFragment = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            99,
            rowType,
            List.of("lucene")
        );
        Stage child = new Stage(1, childFragment, List.of(), null, StageExecutionType.LOCAL);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(child), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();

        AtomicBoolean shardRequestSent = new AtomicBoolean(false);
        ShardRequestClient client = (request, node, listener) -> {
            shardRequestSent.set(true);
            fail("Shard requests should NOT be sent when child fails");
        };

        RuntimeException childError = new RuntimeException("child exploded");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onFailure(childError);

        AtomicReference<Exception> captured = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> fail("should not succeed"), captured::set)
        );

        assertNotNull("Should have received failure", captured.get());
        assertSame("Should propagate the child error", childError, captured.get());
        assertFalse("No shard requests should have been sent", shardRequestSent.get());
    }

    // ─── Task 17: testRegistersAndUnregistersStageExecution ─────────────

    /**
     * Assert {@code state.activeStageExecutions()} is non-empty during
     * execution and empty after completion.
     *
     * Validates: Requirements 3.3
     */
    public void testRegistersAndUnregistersStageExecution() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ShardFanOutStageScheduler scheduler = new ShardFanOutStageScheduler(clusterService);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();

        AtomicBoolean registeredDuringExec = new AtomicBoolean(false);
        ShardRequestClient client = (request, node, listener) -> {
            // Check registration state during execution
            registeredDuringExec.set(state.activeStageExecutions().isEmpty() == false);
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("field_0"), rows), true);
        };

        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        scheduler.schedule(
            stage,
            outputSink,
            client,
            noOpChildren,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e))
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertTrue("Stage execution should have been registered during shard dispatch", registeredDuringExec.get());
        assertTrue("Stage executions should be empty after completion", state.activeStageExecutions().isEmpty());
    }
}
