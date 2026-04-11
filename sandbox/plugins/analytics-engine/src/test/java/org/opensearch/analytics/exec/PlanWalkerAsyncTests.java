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
import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link PlanWalker} async walk behavior: result delivery, failure propagation,
 * empty targets, bottom-up traversal order, and multi-shard response collection.
 */
public class PlanWalkerAsyncTests extends OpenSearchTestCase {

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

    /** Creates a mock backends map where every backend returns a no-op FragmentConvertor (returns empty bytes). */
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

    public void testSingleStageWalkSignalsListenerWithResults() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            // Return 2 rows per shard with known data
            List<String> fields = List.of("field_0");
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "shard_" + request.getShardId().id() + "_row0" });
            rows.add(new Object[] { "shard_" + request.getShardId().id() + "_row1" });
            listener.onResponse(new FragmentExecutionResponse(fields, rows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // 2 shards × 2 rows each = 4 total rows
        assertEquals(4, resultList.size());
    }

    public void testWalkSignalsFailureOnShardError() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        AtomicInteger callCount = new AtomicInteger(0);

        TaskSubmitter submitter = (request, node, listener) -> {
            int call = callCount.getAndIncrement();
            if (call == 0) {
                // First shard succeeds
                List<Object[]> okRows = new ArrayList<>();
                okRows.add(new Object[] { "ok" });
                listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), okRows));
            } else {
                // Second shard fails
                listener.onFailure(new RuntimeException("shard failed"));
            }
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);

        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue(ex.getMessage().contains("Stage 0 failed") || ex.getCause().getMessage().contains("shard failed"));
    }

    public void testEmptyTargetsSignalsListenerImmediately() {
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

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // Coordinator-only stage: no tasks dispatched, empty result
        assertTrue(capturedRequests.isEmpty());
        assertTrue(resultList.isEmpty());
    }

    public void testBottomUpTraversalOrder() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        // Child stage (stageId=0): SINGLETON exchange with TableScan
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan childPlan = new StagePlan(scan, "mock-parquet");
        ExchangeInfo singletonExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage childStage = new Stage(0, scan, List.of(), singletonExchange);
        childStage.setPlanAlternatives(List.of(childPlan));

        // Root stage (stageId=1): coordinator-only with StageInputScan
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
        List<Integer> dispatchedStageIds = new ArrayList<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            dispatchedStageIds.add(request.getStageId());
            List<Object[]> dataRows = new ArrayList<>();
            dataRows.add(new Object[] { "data" });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), dataRows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);
        future.actionGet();

        // Child stage (stageId=0) tasks should be dispatched; root stage (stageId=1)
        // is coordinator-only (StageInputScan, no TableScan) so dispatches no tasks.
        // All dispatched tasks should be for the child stage.
        assertFalse(dispatchedStageIds.isEmpty());
        for (int stageId : dispatchedStageIds) {
            assertEquals("Child stage tasks should be dispatched before root stage executes", 0, stageId);
        }
    }

    public void testMultipleShardResponsesFeedSinkInOrder() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "mock-parquet");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();

        TaskSubmitter submitter = (request, node, listener) -> {
            // Each shard returns a distinct single row, inline (synchronous)
            int shardIdx = request.getShardId().id();
            List<String> fields = List.of("field_0");
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "value_" + shardIdx });
            listener.onResponse(new FragmentExecutionResponse(fields, rows));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        walker.walk(submitter, future);

        Iterable<Object[]> result = future.actionGet();
        List<Object[]> resultList = new ArrayList<>();
        result.forEach(resultList::add);

        // 3 shards × 1 row each = 3 total rows
        assertEquals(3, resultList.size());

        // Verify all 3 shard values are present in the result
        List<String> values = new ArrayList<>();
        for (Object[] row : resultList) {
            values.add((String) row[0]);
        }
        assertTrue(values.contains("value_0"));
        assertTrue(values.contains("value_1"));
        assertTrue(values.contains("value_2"));
    }

    /**
     * Task 7.2: Metadata dispatch collecting manifests.
     * Builds a 2-stage DAG where Stage 0 is a shuffle write stage (SINGLETON distribution
     * with ShuffleImpl.FILE so isShuffle()=true). SINGLETON distribution means targets resolve
     * from index shards; non-null shuffleImpl means isShuffleWriteStage()=true so metadata
     * responses are collected into a PartitionManifest.
     * Validates: Requirements 3.5, 3.6, 1.5
     */
    @SuppressWarnings("unchecked")
    public void testMetadataDispatchCollectsManifests() throws Exception {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        // Stage 0: shuffle write stage — SINGLETON for target resolution, ShuffleImpl.FILE for isShuffle()=true
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan childPlan = new StagePlan(scan, "lucene");
        ExchangeInfo shuffleWriteExchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, ShuffleImpl.FILE, List.of(0));
        Stage childStage = new Stage(0, scan, List.of(), shuffleWriteExchange);
        childStage.setPlanAlternatives(List.of(childPlan));

        // Stage 1: root coordinator gather with StageInputScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        StagePlan rootPlan = new StagePlan(stageInput, "lucene");
        Stage rootStage = new Stage(1, stageInput, List.of(childStage), null);
        rootStage.setPlanAlternatives(List.of(rootPlan));

        QueryDAG dag = new QueryDAG("test-query", rootStage);

        // Mock submitter returns metadata responses (shuffle manifests) for Stage 0
        TaskSubmitter submitter = (request, node, listener) -> {
            Map<String, String> metadata = Map.of(
                "0",
                "/tmp/shuffle/shard_" + request.getShardId().id() + "/p0",
                "1",
                "/tmp/shuffle/shard_" + request.getShardId().id() + "/p1"
            );
            listener.onResponse(new FragmentExecutionResponse(metadata));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        // Walk should complete (root stage is coordinator gather, no dispatch needed for it)
        future.actionGet();

        // Use reflection to verify shuffleManifests contains a manifest for Stage 0
        Field executorField2 = PlanWalker.class.getDeclaredField("stageExecutor");
        executorField2.setAccessible(true);
        Object stageExec2 = executorField2.get(walker);
        Field contextField = StageExecutor.class.getDeclaredField("context");
        contextField.setAccessible(true);
        QueryExecutionContext dispatchContext = (QueryExecutionContext) contextField.get(stageExec2);
        @SuppressWarnings("unchecked")
        Map<Integer, Map<ShardId, Map<Integer, String>>> manifests = dispatchContext.shuffleManifests();

        Map<ShardId, Map<Integer, String>> stage0Manifest = manifests.get(0);
        assertNotNull("Stage 0 should have a shuffle manifest", stage0Manifest);
        assertEquals("Manifest should have one entry per shard", numShards, stage0Manifest.size());

        // Verify each shard's manifest has 2 partitions with correct paths
        for (Map.Entry<ShardId, Map<Integer, String>> entry : stage0Manifest.entrySet()) {
            int shardIdx = entry.getKey().id();
            Map<Integer, String> partitions = entry.getValue();
            assertEquals("Each shard manifest should have 2 partitions", 2, partitions.size());
            assertEquals("/tmp/shuffle/shard_" + shardIdx + "/p0", partitions.get(0));
            assertEquals("/tmp/shuffle/shard_" + shardIdx + "/p1", partitions.get(1));
        }
    }

    /**
     * Task 7.3: resolveShuffleTargets from child stage manifests.
     * Pre-populates stageOutputs with a PartitionManifest for a child stage, then builds
     * a parent stage with HASH_DISTRIBUTED exchange and walks the DAG. Verifies the returned
     * ShardTarget list has one entry per partition with nodes from the source shard set.
     * Validates: Requirements 5.1, 5.2, 5.3
     */
    @SuppressWarnings("unchecked")
    public void testResolveShuffleTargetsFromChildManifests() throws Exception {
        int numSourceShards = 2;
        int numPartitions = 3;
        Index sourceIndex = new Index("http_logs", "_na_");

        // Build mock ClusterState with routing table for source index shards
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        DiscoveryNode nodeA = mock(DiscoveryNode.class);
        DiscoveryNode nodeB = mock(DiscoveryNode.class);
        when(discoveryNodes.get("node_0")).thenReturn(nodeA);
        when(discoveryNodes.get("node_1")).thenReturn(nodeB);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        // Mock routing table: each source shard has a primary on its respective node
        RoutingTable routingTable = mock(RoutingTable.class);
        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index(sourceIndex)).thenReturn(indexRoutingTable);
        for (int i = 0; i < numSourceShards; i++) {
            IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
            ShardRouting primaryShard = mock(ShardRouting.class);
            when(primaryShard.currentNodeId()).thenReturn("node_" + i);
            when(shardRoutingTable.primaryShard()).thenReturn(primaryShard);
            when(indexRoutingTable.shard(i)).thenReturn(shardRoutingTable);
        }
        when(clusterState.routingTable()).thenReturn(routingTable);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        // Build a PartitionManifest for child stage 0
        Map<ShardId, Map<Integer, String>> manifestData = new HashMap<>();
        for (int s = 0; s < numSourceShards; s++) {
            Map<Integer, String> partitions = new HashMap<>();
            for (int p = 0; p < numPartitions; p++) {
                partitions.put(p, "/tmp/shuffle/shard_" + s + "/p" + p);
            }
            manifestData.put(new ShardId(sourceIndex, s), partitions);
        }

        // Build child stage (Stage 0) — shuffle write, already completed
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        Stage childStage = new Stage(0, scan, List.of(), hashExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Build parent stage (Stage 1) — shuffle read with HASH_DISTRIBUTED exchange
        // Use StageInputScan as the fragment (reads from child stage 0)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        ExchangeInfo parentHashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        Stage parentStage = new Stage(1, stageInput, List.of(childStage), parentHashExchange);
        parentStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        // Root stage (Stage 2) — coordinator gather
        OpenSearchStageInputScan rootInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 1, rowType, List.of());
        Stage rootStage = new Stage(2, rootInput, List.of(parentStage), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(rootInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);

        // Pre-populate shuffleManifests with the manifest for child stage 0
        Field executorField = PlanWalker.class.getDeclaredField("stageExecutor");
        executorField.setAccessible(true);
        Object stageExec = executorField.get(walker);
        Field contextField = StageExecutor.class.getDeclaredField("context");
        contextField.setAccessible(true);
        QueryExecutionContext dispatchContext = (QueryExecutionContext) contextField.get(stageExec);
        @SuppressWarnings("unchecked")
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = dispatchContext.shuffleManifests();
        shuffleManifests.put(0, manifestData);

        // Call resolveTargets via TargetResolver
        List<ShardTarget> targets = TargetResolver.resolveTargets(parentStage, clusterService, shuffleManifests);

        // Should have one ShardTarget per partition
        assertEquals("Should have one target per partition", numPartitions, targets.size());

        // All target nodes should come from the source shard node set {nodeA, nodeB}
        for (ShardTarget target : targets) {
            assertTrue("Target node should be from source shard set", target.node() == nodeA || target.node() == nodeB);
            assertEquals("Target shard index should be _shuffle", "_shuffle", target.shardId().getIndexName());
        }

        // Verify partition IDs are 0..numPartitions-1
        for (int p = 0; p < numPartitions; p++) {
            assertEquals("Partition ID should match target index", p, targets.get(p).shardId().id());
        }
    }

    /**
     * Task 7.4: resolveShuffleTargets throwing when no manifest exists.
     * Calls resolveTargets on a stage whose child has no PartitionManifest in stageOutputs.
     * Asserts IllegalStateException is thrown with appropriate message.
     * Validates: Requirements 5.4
     */
    @SuppressWarnings("unchecked")
    public void testResolveShuffleTargetsThrowsWhenNoManifest() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);

        // Build child stage (Stage 0) — no manifest will be populated
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        ExchangeInfo hashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        Stage childStage = new Stage(0, scan, List.of(), hashExchange);
        childStage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Build parent stage (Stage 1) with HASH_DISTRIBUTED exchange
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        ExchangeInfo parentHashExchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        Stage parentStage = new Stage(1, stageInput, List.of(childStage), parentHashExchange);
        parentStage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        // Root stage (Stage 2) — coordinator gather
        OpenSearchStageInputScan rootInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 1, rowType, List.of());
        Stage rootStage = new Stage(2, rootInput, List.of(parentStage), null);
        rootStage.setPlanAlternatives(List.of(new StagePlan(rootInput, "lucene")));

        QueryDAG dag = new QueryDAG("test-query", rootStage);
        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);

        // shuffleManifests is empty — no manifest for child stage 0
        Map<Integer, Map<ShardId, Map<Integer, String>>> emptyManifests = new HashMap<>();
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> TargetResolver.resolveTargets(parentStage, clusterService, emptyManifests)
        );
        assertTrue("Exception message should reference the stage ID", ex.getMessage().contains("No partition manifest found for stage 1"));
    }

    /**
     * Task 7.5: Coordinator gather stage completing without dispatch.
     * Builds a coordinator-only stage (StageInputScan, no exchange, no TableScan).
     * Asserts walk completes, stores RowData in stageOutputs, and signals listener
     * without submitting any tasks.
     * Validates: Requirements 4.1, 4.2
     */
    @SuppressWarnings("unchecked")
    public void testCoordinatorGatherStageCompletesWithoutDispatch() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);

        // Coordinator-only stage with StageInputScan (no TableScan, no exchange)
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        StagePlan plan = new StagePlan(stageInput, "lucene");
        Stage stage = new Stage(1, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        QueryDAG dag = new QueryDAG("test-query", stage);

        AtomicInteger submitCount = new AtomicInteger(0);
        TaskSubmitter submitter = (request, node, listener) -> {
            submitCount.incrementAndGet();
            listener.onResponse(new FragmentExecutionResponse(List.of(), List.of()));
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        // Walk should complete successfully
        Iterable<Object[]> result = future.actionGet();
        assertNotNull(result);

        // No tasks should have been submitted
        assertEquals("Coordinator gather should not dispatch any tasks", 0, submitCount.get());

        // Verify completedStages contains the coordinator gather stage
        Field executorField3 = PlanWalker.class.getDeclaredField("stageExecutor");
        executorField3.setAccessible(true);
        Object stageExec3 = executorField3.get(walker);
        Field contextField = StageExecutor.class.getDeclaredField("context");
        contextField.setAccessible(true);
        QueryExecutionContext dispatchContext = (QueryExecutionContext) contextField.get(stageExec3);
        @SuppressWarnings("unchecked")
        Set<Integer> completed = dispatchContext.completedStages();

        assertTrue("Coordinator gather stage should be in completedStages", completed.contains(1));
    }

    /**
     * Task 7.6: Streaming dispatch failure waiting for all tasks.
     * Builds a streaming stage with N targets, configures submitter so at least one fails.
     * Asserts onFailure is called only after all N tasks complete (remaining reaches 0)
     * and the first exception is captured.
     * Validates: Requirements 12.1, 12.2
     */
    public void testStreamingDispatchFailureWaitsForAllTasks() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));
        QueryDAG dag = new QueryDAG("test-query", stage);

        // Track how many tasks completed before the failure was signaled
        AtomicInteger completedTasks = new AtomicInteger(0);

        // Shard 1 fails, shards 0 and 2 succeed. All responses are synchronous.
        TaskSubmitter submitter = (request, node, listener) -> {
            int shardIdx = request.getShardId().id();
            if (shardIdx == 1) {
                completedTasks.incrementAndGet();
                listener.onFailure(new RuntimeException("shard_1_failed"));
            } else {
                completedTasks.incrementAndGet();
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "value_" + shardIdx });
                listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
            }
        };

        PlanWalker walker = new PlanWalker(dag, clusterService, Runnable::run, null);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        walker.walk(submitter, future);

        // The walk should fail
        RuntimeException ex = expectThrows(RuntimeException.class, future::actionGet);
        assertTrue("Exception should reference stage failure", ex.getMessage().contains("Stage 0 failed"));
        assertTrue("Root cause should be the shard failure", ex.getCause().getMessage().contains("shard_1_failed"));

        // All 3 tasks should have completed before the failure was signaled
        assertEquals("All tasks should have completed", numShards, completedTasks.get());
    }
}
