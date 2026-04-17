/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
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
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the full coordinator dispatch path:
 * {@code PlanWalker → StageExecutionBuilder → StageExecution → AnalyticsSearchTransportService → response → sink}.
 *
 * <p>No cluster, no transport, no IT overhead. Mock {@link AnalyticsSearchTransportService} returns canned
 * responses. Mock {@link ClusterService} provides shard routing. Tests run in milliseconds.
 */
public class CoordinatorDispatchTests extends OpenSearchTestCase {

    private static final String BACKEND = "mock";

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
        rowType = typeFactory.builder().add("f0", SqlTypeName.VARCHAR).add("f1", SqlTypeName.BIGINT).build();
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private OpenSearchTableScan scan(String table) {
        RelOptTable t = mock(RelOptTable.class);
        when(t.getQualifiedName()).thenReturn(List.of("default", table));
        when(t.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), t, List.of(BACKEND), List.of());
    }

    private ClusterService mockCluster(String table, int numShards) {
        Index index = new Index(table, "_na_");
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        List<ShardIterator> iters = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node_" + i);
            when(nodes.get("node_" + i)).thenReturn(node);
            ShardIterator it = mock(ShardIterator.class);
            ShardRouting sr = mock(ShardRouting.class);
            when(sr.shardId()).thenReturn(new ShardId(index, i));
            when(sr.currentNodeId()).thenReturn("node_" + i);
            when(it.nextOrNull()).thenReturn(sr);
            iters.add(it);
        }
        when(state.nodes()).thenReturn(nodes);
        OperationRouting routing = mock(OperationRouting.class);
        when(routing.searchShards(any(), eq(new String[] { table }), isNull(), isNull())).thenReturn(new GroupShardsIterator<>(iters));
        ClusterService cs = mock(ClusterService.class);
        when(cs.state()).thenReturn(state);
        when(cs.operationRouting()).thenReturn(routing);
        return cs;
    }

    /** ClusterService that knows about two tables. */
    private ClusterService mockCluster(String tableA, int shardsA, String tableB, int shardsB) {
        Index indexA = new Index(tableA, "_na_");
        Index indexB = new Index(tableB, "_na_");
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);

        int nodeIdx = 0;
        List<ShardIterator> itersA = new ArrayList<>();
        for (int i = 0; i < shardsA; i++) {
            String nid = "node_" + nodeIdx++;
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn(nid);
            when(nodes.get(nid)).thenReturn(node);
            ShardIterator it = mock(ShardIterator.class);
            ShardRouting sr = mock(ShardRouting.class);
            when(sr.shardId()).thenReturn(new ShardId(indexA, i));
            when(sr.currentNodeId()).thenReturn(nid);
            when(it.nextOrNull()).thenReturn(sr);
            itersA.add(it);
        }
        List<ShardIterator> itersB = new ArrayList<>();
        for (int i = 0; i < shardsB; i++) {
            String nid = "node_" + nodeIdx++;
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn(nid);
            when(nodes.get(nid)).thenReturn(node);
            ShardIterator it = mock(ShardIterator.class);
            ShardRouting sr = mock(ShardRouting.class);
            when(sr.shardId()).thenReturn(new ShardId(indexB, i));
            when(sr.currentNodeId()).thenReturn(nid);
            when(it.nextOrNull()).thenReturn(sr);
            itersB.add(it);
        }
        when(state.nodes()).thenReturn(nodes);
        OperationRouting routing = mock(OperationRouting.class);
        when(routing.searchShards(any(), eq(new String[] { tableA }), isNull(), isNull())).thenReturn(new GroupShardsIterator<>(itersA));
        when(routing.searchShards(any(), eq(new String[] { tableB }), isNull(), isNull())).thenReturn(new GroupShardsIterator<>(itersB));
        ClusterService cs = mock(ClusterService.class);
        when(cs.state()).thenReturn(state);
        when(cs.operationRouting()).thenReturn(routing);
        return cs;
    }

    private static FragmentExecutionResponse mockResponse() {
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(root.getRowCount()).thenReturn(1);
        when(root.getSchema()).thenReturn(new Schema(List.of()));
        when(root.getFieldVectors()).thenReturn(List.of());
        return new FragmentExecutionResponse(root);
    }

    private static FragmentExecutionResponse emptyResponse() {
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(root.getRowCount()).thenReturn(0);
        when(root.getSchema()).thenReturn(new Schema(List.of()));
        when(root.getFieldVectors()).thenReturn(List.of());
        return new FragmentExecutionResponse(root);
    }

    /** Dispatcher that responds immediately with a canned response. */
    private AnalyticsSearchTransportService immediateSubmitter(FragmentExecutionResponse response, ClusterService cs) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                listener.onStreamResponse(response, true);
            }
        };
    }

    /** Dispatcher that counts submissions and responds with a canned response. */
    private AnalyticsSearchTransportService countingSubmitter(FragmentExecutionResponse response, AtomicInteger counter, ClusterService cs) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                counter.incrementAndGet();
                listener.onStreamResponse(response, true);
            }
        };
    }

    /** Dispatcher that fails the Nth request. */
    private AnalyticsSearchTransportService failOnNthSubmitter(
        FragmentExecutionResponse response,
        int failOnN,
        AtomicInteger counter,
        ClusterService cs
    ) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                if (counter.incrementAndGet() == failOnN) {
                    listener.onFailure(new RuntimeException("shard failed [mock]"));
                } else {
                    listener.onStreamResponse(response, true);
                }
            }
        };
    }

    /** Walk a DAG and return collected rows. */
    private List<Object[]> walkAndCollect(QueryDAG dag, ClusterService cs, AnalyticsSearchTransportService dispatcher) throws Exception {
        return walkAndCollect(dag, cs, dispatcher, null);
    }

    private List<Object[]> walkAndCollect(
        QueryDAG dag,
        ClusterService cs,
        AnalyticsSearchTransportService dispatcher,
        Object parentTask
    ) throws Exception {
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, dispatcher, null)
        ).execute(QueryContext.forTest(dag, null), future);
        List<Object[]> rows = new ArrayList<>();
        future.actionGet().forEach(rows::add);
        return rows;
    }

    // ─── Single-stage tests ─────────────────────────────────────────────

    /**
     * 1 stage, 3 shards, 1 row per shard → 3 rows in sink.
     */
    public void testSingleStageFanOut() throws Exception {
        ClusterService cs = mockCluster("t", 3);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("q1", stage);

        FragmentExecutionResponse response = mockResponse();
        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(response, cs));
        // Each shard returns 1 row (mockResponse has rowCount=1), 3 shards
        assertFalse(result.isEmpty());
    }

    /**
     * 1 stage, 1 shard, 0 rows per shard → empty result.
     */
    public void testSingleStageEmptyResult() throws Exception {
        ClusterService cs = mockCluster("t", 1);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("q2", stage);

        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(emptyResponse(), cs));
        assertEquals(0, result.size());
    }

    /**
     * Shard failure → "Stage 0 failed" wrapping.
     */
    public void testSingleStageShardFailure() {
        ClusterService cs = mockCluster("t", 3);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("q3", stage);

        AtomicInteger counter = new AtomicInteger();
        FragmentExecutionResponse response = mockResponse();
        AnalyticsSearchTransportService sub = failOnNthSubmitter(response, 2, counter, cs);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    /**
     * All shards fail → "Stage 0 failed", first exception captured.
     */
    public void testSingleStageAllShardsFail() {
        ClusterService cs = mockCluster("t", 3);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("q4", stage);

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                listener.onFailure(new RuntimeException("boom"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
        assertEquals("boom", ex.getCause().getCause().getMessage());
    }

    // ─── Two-stage tests ────────────────────────────────────────────────

    /**
     * 2-stage: child stage (3 shards) → coordinator gather root.
     * Rows from child arrive in rootSink.
     */
    public void testTwoStageCoordinatorGather() throws Exception {
        ClusterService cs = mockCluster("t", 3);
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        OpenSearchTableScan s = scan("t");
        Stage child = new Stage(0, s, List.of(), exchange);
        child.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        Stage root = new Stage(1, null, List.of(child), null);

        QueryDAG dag = new QueryDAG("q5", root);
        FragmentExecutionResponse response = mockResponse();
        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(response, cs));

        // 3 shards respond
        assertFalse(result.isEmpty());
    }

    /**
     * 2-stage: child stage fails → root never dispatches, listener gets failure.
     */
    public void testTwoStageChildFailurePropagates() {
        ClusterService cs = mockCluster("t", 2);
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        OpenSearchTableScan s = scan("t");
        Stage child = new Stage(0, s, List.of(), exchange);
        child.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        Stage root = new Stage(1, null, List.of(child), null);

        QueryDAG dag = new QueryDAG("q6", root);
        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                listener.onFailure(new RuntimeException("child boom"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Three-stage: parallel join ─────────────────────────────────────

    public void testThreeStageParallelJoin() throws Exception {
        ClusterService cs = mockCluster("orders", 2, "customers", 3);

        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());

        OpenSearchTableScan scanOrders = scan("orders");
        Stage stageOrders = new Stage(0, scanOrders, List.of(), exchange);
        stageOrders.setPlanAlternatives(List.of(new StagePlan(scanOrders, BACKEND)));

        OpenSearchTableScan scanCustomers = scan("customers");
        Stage stageCustomers = new Stage(1, scanCustomers, List.of(), exchange);
        stageCustomers.setPlanAlternatives(List.of(new StagePlan(scanCustomers, BACKEND)));

        Stage root = new Stage(2, null, List.of(stageOrders, stageCustomers), null);

        QueryDAG dag = new QueryDAG("join-q", root);
        FragmentExecutionResponse response = mockResponse();

        AtomicInteger submissions = new AtomicInteger();
        List<Object[]> result = walkAndCollect(dag, cs, countingSubmitter(response, submissions, cs));

        // 2 shards (orders) + 3 shards (customers) = 5 submissions
        assertEquals(5, submissions.get());
    }

    public void testThreeStageParallelJoinOneChildFails() {
        ClusterService cs = mockCluster("orders", 2, "customers", 3);

        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());

        OpenSearchTableScan scanOrders = scan("orders");
        Stage stageOrders = new Stage(0, scanOrders, List.of(), exchange);
        stageOrders.setPlanAlternatives(List.of(new StagePlan(scanOrders, BACKEND)));

        OpenSearchTableScan scanCustomers = scan("customers");
        Stage stageCustomers = new Stage(1, scanCustomers, List.of(), exchange);
        stageCustomers.setPlanAlternatives(List.of(new StagePlan(scanCustomers, BACKEND)));

        Stage root = new Stage(2, null, List.of(stageOrders, stageCustomers), null);

        QueryDAG dag = new QueryDAG("join-fail-q", root);

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                if (req.getShardId().getIndex().getName().equals("orders")) {
                    listener.onFailure(new RuntimeException("orders shard failed"));
                } else {
                    listener.onStreamResponse(mockResponse(), true);
                }
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Three-stage: sequential chain ──────────────────────────────────

    public void testThreeStageSequentialChain() throws Exception {
        ClusterService cs = mockCluster("t", 3, "t2", 2);

        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());

        OpenSearchTableScan scan0 = scan("t");
        Stage stage0 = new Stage(0, scan0, List.of(), exchange);
        stage0.setPlanAlternatives(List.of(new StagePlan(scan0, BACKEND)));

        OpenSearchTableScan scan1 = scan("t2");
        Stage stage1 = new Stage(1, scan1, List.of(stage0), exchange);
        stage1.setPlanAlternatives(List.of(new StagePlan(scan1, BACKEND)));

        Stage root = new Stage(2, null, List.of(stage1), null);

        QueryDAG dag = new QueryDAG("chain-q", root);

        List<String> dispatchOrder = Collections.synchronizedList(new ArrayList<>());
        FragmentExecutionResponse response = mockResponse();

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                dispatchOrder.add("stage" + req.getStageId() + "_shard" + req.getShardId().id());
                listener.onStreamResponse(response, true);
            }
        };

        List<Object[]> result = walkAndCollect(dag, cs, sub);

        // Stage 0 shards come first (3), then Stage 1 shards (2)
        assertEquals(5, dispatchOrder.size());
        for (int i = 0; i < 3; i++) {
            assertTrue("First 3 dispatches should be stage0, got: " + dispatchOrder, dispatchOrder.get(i).startsWith("stage0"));
        }
        for (int i = 3; i < 5; i++) {
            assertTrue("Last 2 dispatches should be stage1, got: " + dispatchOrder, dispatchOrder.get(i).startsWith("stage1"));
        }
    }

    public void testThreeStageSequentialLeafFailure() {
        ClusterService cs = mockCluster("t", 2, "t2", 2);

        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        OpenSearchTableScan scan0 = scan("t");
        Stage stage0 = new Stage(0, scan0, List.of(), exchange);
        stage0.setPlanAlternatives(List.of(new StagePlan(scan0, BACKEND)));

        OpenSearchTableScan scan1 = scan("t2");
        Stage stage1 = new Stage(1, scan1, List.of(stage0), exchange);
        stage1.setPlanAlternatives(List.of(new StagePlan(scan1, BACKEND)));

        Stage root = new Stage(2, null, List.of(stage1), null);
        QueryDAG dag = new QueryDAG("chain-fail-q", root);

        AtomicInteger submissions = new AtomicInteger();
        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                submissions.incrementAndGet();
                listener.onFailure(new RuntimeException("stage " + req.getStageId() + " boom"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
        assertEquals("Only stage 0 shards dispatched (2)", 2, submissions.get());
    }

    // ─── Cancellation ───────────────────────────────────────────────────

    public void testCancellationReturnsCleanException() {
        ClusterService cs = mockCluster("t", 2);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("cancel-q", stage);

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTaskArg,
                PendingExecutions _pending
            ) {
                listener.onFailure(new TaskCancelledException("task cancelled [mock]"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        // Without a real AnalyticsQueryTask, cancellation is wrapped as stage failure
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    public void testBottomUpCancellationWrappedAsStageFailure() {
        ClusterService cs = mockCluster("t", 2);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("bottom-cancel-q", stage);

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTaskArg,
                PendingExecutions _pending
            ) {
                listener.onFailure(new TaskCancelledException("circuit breaker [mock]"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertFalse("Should NOT be TaskCancelledException", ex.getCause() instanceof TaskCancelledException);
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    public void testNullParentTaskFallsBackToWrapping() {
        ClusterService cs = mockCluster("t", 1);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("null-task-q", stage);

        AnalyticsSearchTransportService sub = new AnalyticsSearchTransportService(mock(TransportService.class), cs) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest req,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTaskArg,
                PendingExecutions _pending
            ) {
                listener.onFailure(new TaskCancelledException("cancelled"));
            }
        };

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        new QueryScheduler(
            new StageExecutionBuilder(cs, sub, null)
        ).execute(QueryContext.forTest(dag, null), future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Submission counting ────────────────────────────────────────────

    public void testSubmissionCountMatchesShards() throws Exception {
        ClusterService cs = mockCluster("t", 5);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("count-q", stage);

        AtomicInteger submissions = new AtomicInteger();
        FragmentExecutionResponse response = mockResponse();
        walkAndCollect(dag, cs, countingSubmitter(response, submissions, cs));

        assertEquals(5, submissions.get());
    }

    public void testThreeStageParallelSubmissionCount() throws Exception {
        ClusterService cs = mockCluster("a", 4, "b", 6);
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());

        OpenSearchTableScan scanA = scan("a");
        Stage sA = new Stage(0, scanA, List.of(), exchange);
        sA.setPlanAlternatives(List.of(new StagePlan(scanA, BACKEND)));

        OpenSearchTableScan scanB = scan("b");
        Stage sB = new Stage(1, scanB, List.of(), exchange);
        sB.setPlanAlternatives(List.of(new StagePlan(scanB, BACKEND)));

        Stage root = new Stage(2, null, List.of(sA, sB), null);
        QueryDAG dag = new QueryDAG("pcount-q", root);

        AtomicInteger submissions = new AtomicInteger();
        FragmentExecutionResponse response = mockResponse();
        walkAndCollect(dag, cs, countingSubmitter(response, submissions, cs));

        assertEquals(10, submissions.get());
    }
}
