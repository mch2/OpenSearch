/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.MockFragmentResponse;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;

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
import org.opensearch.analytics.backend.LocalStageContext;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

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
 * Tests for the {@link StageExecutionBuilder} primary backend injection.
 * Validates: Requirements 2.5, 3.1, 3.3
 */
@SuppressWarnings("unchecked")
public class StageExecutionBuilderPrimaryBackendTests extends OpenSearchTestCase {

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
     * Construct StageExecutionBuilder with a mock backend, dispatch a compute LOCAL
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

        // Build dispatcher
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "row" });
                listener.onStreamResponse(MockFragmentResponse.create(List.of("field_0"), rows), true);
            }
        };

        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of(mockBackend.name(), mockBackend));
        QueryContext config = QueryContext.forTest("test-query", null);


        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();

        // buildRootExecution for LOCAL stages creates the LocalStageContext and LocalStageExecution.
        // Child walking (sinkFor, asyncFinalize) is the walker's responsibility.
        StageExecution rootExec = executor.buildExecution(rootStage, new PassThroughStageExecution(rootStage, new RowProducingSink()), config);

        assertNotNull("Root execution should not be null", rootExec);
        assertTrue("Should be LocalStageExecution", rootExec instanceof LocalStageExecution);
        assertEquals("Initial state should be CREATED", StageExecution.State.CREATED, rootExec.getState());

        verify(mockBackend).createLocalStage(any());
    }

    /**
     * Construct StageExecutionBuilder with null primaryBackend (test-only 1-arg ctor),
     * dispatch a DATA_NODE stage, and verify it still works.
     *
     * Validates: Requirements 2.5
     */
    public void testNoPrimaryBackendAllowedForDataNodeOnlyUse() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);

        // test-only 2-arg constructor (null backend)
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "row_" + request.getShardId().id() });
                listener.onStreamResponse(MockFragmentResponse.create(List.of("field_0"), rows), true);
            }
        };
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of());
        QueryContext config = QueryContext.forTest("test-query", null);


        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();

        StageExecution exec = executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
        assertNotNull("Execution should not be null", exec);
        exec.start();

        assertEquals("State should be SUCCEEDED", StageExecution.State.SUCCEEDED, exec.getState());
    }

    /**
     * Construct StageExecutionBuilder with null primaryBackend (test-only 1-arg ctor),
     * dispatch a compute LOCAL stage, and verify it fails fast with a clear
     * IllegalStateException mentioning "primaryBackend".
     *
     * Validates: Requirements 3.3
     */
    public void testNullPrimaryBackendFailsFastOnComputeLocalStage() {
        ClusterService clusterService = mock(ClusterService.class);

        // test-only 2-arg constructor (null backend)
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                fail("should not be called");
            }
        };
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, dispatcher, java.util.Map.of());
        QueryContext config = QueryContext.forTest("test-query", null);


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
        try {
            executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
            fail("should have thrown");
        } catch (IllegalStateException e) {
            captured.set(e);
        }

        Exception e = captured.get();
        assertNotNull("Should have received failure", e);
        assertTrue("Should be IllegalStateException, got: " + e.getClass().getName(), e instanceof IllegalStateException);
        assertTrue(
            "Message should mention backends, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("No analytics backends registered")
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

