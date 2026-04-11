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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link StageExecutor#dispatch} correctly delegates to
 * {@link StageExecution} for non-coordinator-gather stages and handles
 * coordinator-gather stages inline.
 *
 * Validates: Requirements 6.1, 6.2, 6.3, 6.4, 7.3
 */
@SuppressWarnings("unchecked")
public class StageExecutorDelegationTests extends OpenSearchTestCase {

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
     * Coordinator-gather stage (exchangeInfo=null, tableName=null) is handled
     * inline by StageExecutor: listener is called with onResponse, and no
     * submissions go to the submitter.
     *
     * Validates: Requirements 6.1
     */
    public void testDispatchForCoordinatorGatherDoesNotCreateStageDispatchTask() {
        ClusterService clusterService = mock(ClusterService.class);
        ExchangeSink rootSink = new SimpleExchangeSink();

        StageExecutor executor = new StageExecutor("test-query", clusterService, Runnable::run, rootSink, null);

        // Coordinator-gather stage: StageInputScan, no exchange, no TableScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage stage = new Stage(1, stageInput, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(stageInput, "lucene")));

        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = (request, node, listener) -> submissions.incrementAndGet();

        AtomicReference<Void> responseRef = new AtomicReference<>();
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseRef.set(v);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        executor.dispatch(stage, submitter, listener);

        // Verify listener.onResponse was called (responseRef was set — no exception)
        assertNull("listener.onFailure should not have been called", failureRef.get());
        // Verify no submissions
        assertEquals("No submissions should have been made for coordinator-gather", 0, submissions.get());
    }

    /**
     * Non-gather stage with 3 target shards delegates to StageExecution.
     * Verifies 3 submissions are captured and driving 3 responses completes
     * the stage with listener.onResponse called.
     *
     * Validates: Requirements 6.2, 6.3, 6.4, 7.3
     */
    public void testDispatchForDataNodeStageCreatesAndRunsTask() {
        int numShards = 3;
        ClusterService clusterService = buildMockClusterService("http_logs", numShards);
        ExchangeSink rootSink = new SimpleExchangeSink();

        StageExecutor executor = new StageExecutor("test-query", clusterService, Runnable::run, rootSink, null);

        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        StagePlan plan = new StagePlan(scan, "lucene");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(plan));

        // Capturing submitter that records submissions and responds synchronously
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = (request, node, listener) -> {
            submissions.incrementAndGet();
            // Respond immediately with row data
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] { "row_" + request.getShardId().id() });
            listener.onResponse(new FragmentExecutionResponse(List.of("field_0"), rows));
        };

        AtomicReference<Boolean> responseCalled = new AtomicReference<>(false);
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        executor.dispatch(stage, submitter, listener);

        // Verify 3 submissions were made (one per shard)
        assertEquals("Submitter should have received 3 calls", numShards, submissions.get());
        // Verify listener.onResponse was called
        assertTrue("listener.onResponse should have been called", responseCalled.get());
        assertNull("listener.onFailure should not have been called", failureRef.get());
    }
}
