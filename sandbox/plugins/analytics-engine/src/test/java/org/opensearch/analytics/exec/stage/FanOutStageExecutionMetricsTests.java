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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link ShardScanStageExecution} correctly counts rows via
 * externally-supplied {@link StageMetrics}.
 *
 * Validates: Requirements 3.1, 3.3
 */
@SuppressWarnings("unchecked")
public class FanOutStageExecutionMetricsTests extends OpenSearchTestCase {

    /**
     * 3 shard dispatches, each returning 10 rows → metrics.rowsProcessed == 30.
     *
     * Validates: Requirements 3.1, 3.3
     */
    public void testRowsProcessedCountedAcrossShards() {
        int numTargets = 3;
        int rowsPerShard = 10;
        int stageId = 0;

        Stage stage = mockStage(numTargets, stageId);
        List<ShardTarget> targets = buildTargets(numTargets);

        // Use the raw sink directly (RowOutput wrapper no longer exists)
        RowProducingSink rawSink = new RowProducingSink();

        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), mock(ClusterService.class)) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                captured.add(listener);
            }
        };

        Function<ShardTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            "test-query",
            stage.getStageId(),
            target.shardId(),
            List.of()
        );

        QueryContext config = QueryContext.forTest("test-query", null);

        ShardScanStageExecution exec = new ShardScanStageExecution(
            stage,
            config,
            rawSink,
            targets,
            requestBuilder,
            dispatcher
        );

        exec.start();
        assertEquals("All 3 targets must be dispatched", numTargets, captured.size());

        // Each shard returns a response with rowsPerShard rows
        for (int i = 0; i < numTargets; i++) {
            List<Object[]> rows = new ArrayList<>();
            for (int r = 0; r < rowsPerShard; r++) {
                rows.add(new Object[] { "value_" + r });
            }
            FragmentExecutionResponse response = MockFragmentResponse.create(List.of("field"), rows);
            captured.get(i).onStreamResponse(response, true);
        }

        // Row counts are tracked by the execution's own AbstractStageExecution.metrics.
        assertEquals(
            "rowsProcessed must equal total rows across all shards",
            numTargets * rowsPerShard,
            exec.getMetrics().getRowsProcessed()
        );
        // Task completion counts are tracked by the execution's internal metrics.
        assertEquals("tasksCompleted must be 3", numTargets, exec.getMetrics().getTasksCompleted());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int numTargets, int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        when(stage.isShuffleWrite()).thenReturn(false);
        return stage;
    }

    private List<ShardTarget> buildTargets(int count) {
        List<ShardTarget> targets = new ArrayList<>();
        Index index = new Index("test_index", "_na_");
        for (int i = 0; i < count; i++) {
            ShardId shardId = new ShardId(index, i);
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node_" + i);
            targets.add(new ShardTarget(shardId, node));
        }
        return targets;
    }
}

