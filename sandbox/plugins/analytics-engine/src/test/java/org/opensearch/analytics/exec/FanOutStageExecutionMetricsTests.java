/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link FanOutStageExecution} correctly counts rows via
 * externally-supplied {@link StageMetrics} and a {@link MetricsInstrumentedSink}.
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

        QueryState state = new QueryState();
        StageMetrics metrics = state.metricsFor(stageId);

        Stage stage = mockStage(numTargets, stageId);
        List<ShardTarget> targets = buildTargets(numTargets);

        // Wrap the output sink with MetricsInstrumentedSink (same as StageExecutor does)
        SimpleExchangeSink rawSink = new SimpleExchangeSink();
        MetricsInstrumentedSink instrumentedSink = new MetricsInstrumentedSink(metrics, rawSink);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = (request, node, listener) -> captured.add(listener);

        ActionListener<Void> stageListener = mock(ActionListener.class);

        FanOutStageExecution exec = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            state.rootSink(),
            new SinkFeedingHandler(instrumentedSink),
            state.completedStages(),
            state.shuffleManifests(),
            client,
            stageListener,
            metrics
        );

        exec.run();
        assertEquals("All 3 targets must be dispatched", numTargets, captured.size());

        // Each shard returns a response with rowsPerShard rows
        for (int i = 0; i < numTargets; i++) {
            List<Object[]> rows = new ArrayList<>();
            for (int r = 0; r < rowsPerShard; r++) {
                rows.add(new Object[] { "value_" + r });
            }
            FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("field"), rows);
            captured.get(i).onStreamResponse(response, true);
        }

        assertEquals(
            "rowsProcessed must equal total rows across all shards",
            numTargets * rowsPerShard,
            state.metricsFor(stageId).getRowsProcessed()
        );
        assertEquals("tasksCompleted must be 3", numTargets, metrics.getTasksCompleted());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int initialBatchSize, int stageId) {
        TerminationDecider decider = mock(TerminationDecider.class);
        when(decider.initialBatchSize(anyInt())).thenReturn(initialBatchSize);
        when(decider.shouldTerminate(any(), anyInt(), anyInt())).thenReturn(false);

        Stage stage = mock(Stage.class);
        when(stage.getTerminationDecider()).thenReturn(decider);
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
            targets.add(new ShardTarget(shardId, node));
        }
        return targets;
    }
}
