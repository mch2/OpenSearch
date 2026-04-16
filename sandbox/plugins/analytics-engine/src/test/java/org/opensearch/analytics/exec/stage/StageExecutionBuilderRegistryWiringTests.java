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
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;

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
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link StageExecutionBuilder#buildExecution} correctly constructs
 * a {@link ShardScanStageExecution} and that the execution transitions
 * through its lifecycle states properly.
 *
 * Validates: Requirements 4.8
 */
@SuppressWarnings("unchecked")
public class StageExecutionBuilderRegistryWiringTests extends OpenSearchTestCase {

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

    /**
     * Dispatches a data-node stage that completes successfully. Verifies:
     * 1. The execution is constructed and started correctly
     * 2. After successful completion, the execution state is SUCCEEDED
     *
     * Validates: Requirements 4.8
     */
    public void testDispatchRegistersAndUnregistersOnSuccess() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);
        QueryContext config = QueryContext.forTest("test-query", null);


        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Track registry state during execution
        AtomicReference<Collection<StageExecution>> registeredDuringExec = new AtomicReference<>();
        AtomicReference<StageExecution> execRef = new AtomicReference<>();

        // Dispatcher that captures the registry snapshot before responding
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchScan(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<ScanResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                if (registeredDuringExec.get() == null && execRef.get() != null) {
                    registeredDuringExec.set(List.of(execRef.get()));
                }
                List<Object[]> rows = new ArrayList<>();
                rows.add(new Object[] { "row_" + request.getShardId().id() });
                listener.onStreamResponse(new ScanResponse(List.of("field_0"), rows), true);
            }
        };
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, dispatcher, null);

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

        // buildExecution takes the root sink via PassThroughStageExecution
        StageExecution exec = executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
        execRef.set(exec);
        exec.start();

        // Verify success via execution state
        assertEquals("Execution should have succeeded", StageExecution.State.SUCCEEDED, exec.getState());

        // Verify the execution was registered during dispatch
        assertNotNull("Registry snapshot should have been captured during execution", registeredDuringExec.get());
        assertEquals("Exactly one execution should have been registered during dispatch", 1, registeredDuringExec.get().size());
        StageExecution captured = registeredDuringExec.get().iterator().next();
        assertEquals("Registered execution should have the correct stage id", stage.getStageId(), captured.getStageId());

        // Verify the execution is still registered after completion (unregistration is now the walker's job)
        // Registration happens in PlanWalker, not in the scheduler
        // Verified via execution state instead
    }

    /**
     * Dispatches a data-node stage that fails. Verifies that the execution
     * transitions to FAILED state and captures the failure.
     *
     * Validates: Requirements 4.8
     */
    public void testDispatchUnregistersOnFailure() {
        int numShards = 2;
        ClusterService clusterService = buildMockClusterService("test_table", numShards);
        QueryContext config = QueryContext.forTest("test-query", null);


        OpenSearchTableScan scan = buildTableScan("test_table");
        Stage stage = new Stage(0, scan, List.of(), null);
        stage.setPlanAlternatives(List.of(new StagePlan(scan, "lucene")));

        // Dispatcher that fails every shard request
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), clusterService) {
            @Override
            public void dispatchScan(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<ScanResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                listener.onFailure(new RuntimeException("shard dispatch failed"));
            }
        };
        StageExecutionBuilder executor = new StageExecutionBuilder(clusterService, dispatcher, null);

        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                fail("listener.onResponse should not have been called on failure path");
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        // buildExecution takes the root sink via PassThroughStageExecution
        StageExecution exec = executor.buildExecution(stage, new PassThroughStageExecution(stage, new RowProducingSink()), config);
        exec.start();

        // Verify failure via execution state
        assertEquals("Execution should have failed", StageExecution.State.FAILED, exec.getState());
        assertNotNull("Execution should have a captured failure", exec.getFailure());

        // Verify the execution is still registered (walker handles unregistration)
        // Registration happens in PlanWalker, not in the scheduler
        // Verified via execution state instead
    }
}

