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
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

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
 * {@code PlanWalker → StageExecutor → StageExecution → ShardRequestClient → response → sink}.
 *
 * <p>No cluster, no transport, no IT overhead. Mock {@link ShardRequestClient} returns canned
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

    private CancellableTask mockParentTask(boolean cancelled) {
        CancellableTask task = mock(CancellableTask.class);
        when(task.isCancelled()).thenReturn(cancelled);
        return task;
    }

    /** ShardRequestClient that responds immediately with canned rows. */
    private ShardRequestClient immediateSubmitter(List<Object[]> rows) {
        return (request, node, listener) -> { listener.onStreamResponse(new FragmentExecutionResponse(List.of("f0", "f1"), rows), true); };
    }

    /** ShardRequestClient that counts submissions and responds with canned rows. */
    private ShardRequestClient countingSubmitter(List<Object[]> rows, AtomicInteger counter) {
        return (request, node, listener) -> {
            counter.incrementAndGet();
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("f0", "f1"), rows), true);
        };
    }

    /** ShardRequestClient that fails the Nth request. */
    private ShardRequestClient failOnNthSubmitter(List<Object[]> rows, int failOnN, AtomicInteger counter) {
        return (request, node, listener) -> {
            if (counter.incrementAndGet() == failOnN) {
                listener.onFailure(new RuntimeException("shard failed [mock]"));
            } else {
                listener.onStreamResponse(new FragmentExecutionResponse(List.of("f0", "f1"), rows), true);
            }
        };
    }

    /** Walk a DAG and return collected rows. */
    private List<Object[]> walkAndCollect(QueryDAG dag, ClusterService cs, ShardRequestClient client) throws Exception {
        return walkAndCollect(dag, cs, client, null);
    }

    private List<Object[]> walkAndCollect(QueryDAG dag, ClusterService cs, ShardRequestClient client, CancellableTask parentTask)
        throws Exception {
        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, parentTask), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(client, future);
        List<Object[]> rows = new ArrayList<>();
        future.actionGet().forEach(rows::add);
        return rows;
    }

    // ─── Single-stage tests ─────────────────────────────────────────────

    /**
     * 1 stage, 3 shards, 2 rows per shard → 6 rows in sink.
     */
    public void testSingleStageFanOut() throws Exception {
        ClusterService cs = mockCluster("t", 3);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("q1", stage);

        List<Object[]> rows = java.util.Arrays.asList(new Object[] { "a", 1L }, new Object[] { "b", 2L });
        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(rows));
        assertEquals(6, result.size());
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

        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(List.of()));
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
        List<Object[]> rows = Collections.singletonList(new Object[] { "ok", 1L });
        ShardRequestClient sub = failOnNthSubmitter(rows, 2, counter);

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

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

        ShardRequestClient sub = (req, node, listener) -> listener.onFailure(new RuntimeException("boom"));

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

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
        Stage root = new Stage(1, null, List.of(child), null); // coordinator gather

        QueryDAG dag = new QueryDAG("q5", root);
        List<Object[]> rows = Collections.singletonList(new Object[] { "v", 10L });
        List<Object[]> result = walkAndCollect(dag, cs, immediateSubmitter(rows));

        assertEquals("3 shards × 1 row", 3, result.size());
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
        ShardRequestClient sub = (req, node, listener) -> listener.onFailure(new RuntimeException("child boom"));

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Three-stage: parallel join ─────────────────────────────────────

    /**
     * 3-stage parallel join:
     *   Stage 2 [LOCAL]
     *     ├─ Stage 0 [DATA_NODE] orders (2 shards)
     *     └─ Stage 1 [DATA_NODE] customers (3 shards)
     *
     * Both children dispatch in parallel, root gathers all rows.
     */
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
        List<Object[]> rows = Collections.singletonList(new Object[] { "row", 1L });

        AtomicInteger submissions = new AtomicInteger();
        List<Object[]> result = walkAndCollect(dag, cs, countingSubmitter(rows, submissions));

        // 2 shards (orders) + 3 shards (customers) = 5 submissions, 5 rows
        assertEquals(5, submissions.get());
        assertEquals(5, result.size());
    }

    /**
     * 3-stage parallel join: one child fails, other succeeds.
     * The query still fails — any child failure propagates.
     */
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

        // Fail all shards for "orders", succeed for "customers"
        ShardRequestClient sub = (req, node, listener) -> {
            if (req.getShardId().getIndex().getName().equals("orders")) {
                listener.onFailure(new RuntimeException("orders shard failed"));
            } else {
                listener.onStreamResponse(
                    new FragmentExecutionResponse(List.of("f0", "f1"), Collections.singletonList(new Object[] { "ok", 1L })),
                    true
                );
            }
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Three-stage: sequential chain ──────────────────────────────────

    /**
     * 3-stage sequential chain:
     *   Stage 2 [LOCAL]
     *     └─ Stage 1 [DATA_NODE] (2 shards, depends on Stage 0)
     *          └─ Stage 0 [DATA_NODE] (3 shards, leaf scan)
     *
     * Stage 0 dispatches first, then Stage 1, then root gathers.
     * Verifies bottom-up ordering.
     */
    public void testThreeStageSequentialChain() throws Exception {
        ClusterService cs = mockCluster("t", 3, "t2", 2);

        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());

        // Stage 0: leaf scan on "t" (3 shards)
        OpenSearchTableScan scan0 = scan("t");
        Stage stage0 = new Stage(0, scan0, List.of(), exchange);
        stage0.setPlanAlternatives(List.of(new StagePlan(scan0, BACKEND)));

        // Stage 1: scan on "t2" (2 shards), depends on Stage 0
        OpenSearchTableScan scan1 = scan("t2");
        Stage stage1 = new Stage(1, scan1, List.of(stage0), exchange);
        stage1.setPlanAlternatives(List.of(new StagePlan(scan1, BACKEND)));

        // Stage 2: coordinator gather
        Stage root = new Stage(2, null, List.of(stage1), null);

        QueryDAG dag = new QueryDAG("chain-q", root);

        // Track dispatch order
        List<String> dispatchOrder = Collections.synchronizedList(new ArrayList<>());
        List<Object[]> rows = Collections.singletonList(new Object[] { "v", 1L });

        ShardRequestClient sub = (req, node, listener) -> {
            dispatchOrder.add("stage" + req.getStageId() + "_shard" + req.getShardId().id());
            listener.onStreamResponse(new FragmentExecutionResponse(List.of("f0", "f1"), rows), true);
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

        // All 5 shard responses feed the sink
        assertEquals(5, result.size());
    }

    /**
     * 3-stage sequential chain: leaf stage fails → middle stage never dispatches.
     */
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
        ShardRequestClient sub = (req, node, listener) -> {
            submissions.incrementAndGet();
            listener.onFailure(new RuntimeException("stage " + req.getStageId() + " boom"));
        };

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        // Stage 0 fails, Stage 1 never dispatches
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
        assertEquals("Only stage 0 shards dispatched (2)", 2, submissions.get());
    }

    // ─── Cancellation ───────────────────────────────────────────────────

    /**
     * Top-down cancellation: parentTask.isCancelled()=true → TaskCancelledException.
     */
    public void testCancellationReturnsCleanException() {
        ClusterService cs = mockCluster("t", 2);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("cancel-q", stage);

        CancellableTask parentTask = mockParentTask(true);

        // All shards respond with TaskCancelledException (simulating data-node-side cancellation)
        ShardRequestClient sub = (req, node, listener) -> listener.onFailure(new TaskCancelledException("task cancelled [mock]"));

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, parentTask), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof TaskCancelledException);
        assertEquals("query cancelled", ex.getCause().getMessage());
    }

    /**
     * Bottom-up cancellation: parentTask NOT cancelled, shard returns TaskCancelledException
     * → "Stage 0 failed" wrapping (not clean TaskCancelledException).
     */
    public void testBottomUpCancellationWrappedAsStageFailure() {
        ClusterService cs = mockCluster("t", 2);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("bottom-cancel-q", stage);

        CancellableTask parentTask = mockParentTask(false); // NOT cancelled

        ShardRequestClient sub = (req, node, listener) -> listener.onFailure(new TaskCancelledException("circuit breaker [mock]"));

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, parentTask), new QueryState(), new StageExecutor(cs));
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertFalse("Should NOT be TaskCancelledException", ex.getCause() instanceof TaskCancelledException);
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    /**
     * Null parentTask → falls back to "Stage N failed" wrapping on any failure.
     */
    public void testNullParentTaskFallsBackToWrapping() {
        ClusterService cs = mockCluster("t", 1);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("null-task-q", stage);

        ShardRequestClient sub = (req, node, listener) -> listener.onFailure(new TaskCancelledException("cancelled"));

        PlanWalker walker = new PlanWalker(QueryContext.forTest(dag, null), new QueryState(), new StageExecutor(cs)); // null parent
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(sub, future);

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("Stage 0 failed"));
    }

    // ─── Submission counting ────────────────────────────────────────────

    /**
     * Verify exact submission count: 3 shards → 3 submissions.
     */
    public void testSubmissionCountMatchesShards() throws Exception {
        ClusterService cs = mockCluster("t", 5);
        OpenSearchTableScan s = scan("t");
        Stage stage = new Stage(0, s, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(s, BACKEND)));
        QueryDAG dag = new QueryDAG("count-q", stage);

        AtomicInteger submissions = new AtomicInteger();
        List<Object[]> rows = Collections.singletonList(new Object[] { "x", 1L });
        walkAndCollect(dag, cs, countingSubmitter(rows, submissions));

        assertEquals(5, submissions.get());
    }

    /**
     * 3-stage parallel join: total submissions = shards(A) + shards(B).
     */
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
        List<Object[]> rows = Collections.singletonList(new Object[] { "r", 1L });
        walkAndCollect(dag, cs, countingSubmitter(rows, submissions));

        assertEquals(10, submissions.get());
    }
}
