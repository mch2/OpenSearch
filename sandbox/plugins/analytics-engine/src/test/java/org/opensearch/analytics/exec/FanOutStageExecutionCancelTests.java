/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FanOutStageExecution#cancel(String)}. Validates that
 * cancel transitions state to CANCELLED, fires the listener with
 * {@link TaskCancelledException}, is idempotent, and causes in-flight
 * responses to be ignored.
 *
 * Validates: Requirements 4.5
 */
@SuppressWarnings("unchecked")
public class FanOutStageExecutionCancelTests extends OpenSearchTestCase {

    /**
     * Cancel from RUNNING state: transitions to CANCELLED and fires
     * listener with TaskCancelledException.
     *
     * Validates: Requirements 4.5
     */
    public void testCancelFromRunningTransitionsAndFiresListener() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildExec(numTargets, listener, captured);

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        task.cancel("test reason");

        assertEquals("State must be CANCELLED", StageExecution.State.CANCELLED, task.getState());
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        verify(listener, never()).onResponse(any());

        Exception ex = captor.getValue();
        assertTrue("Must be TaskCancelledException", ex instanceof TaskCancelledException);
        assertEquals("test reason", ex.getMessage());
    }

    /**
     * Double cancel is idempotent: listener is called exactly once.
     *
     * Validates: Requirements 4.5
     */
    public void testDoubleCancelIdempotent() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildExec(numTargets, listener, captured);

        task.run();
        task.cancel("first cancel");
        task.cancel("second cancel");

        assertEquals(StageExecution.State.CANCELLED, task.getState());
        verify(listener, times(1)).onFailure(any());
        verify(listener, never()).onResponse(any());
    }

    /**
     * After cancel, in-flight response callbacks are no-ops because
     * {@code isTerminated()} returns true for CANCELLED state.
     *
     * Validates: Requirements 4.5
     */
    public void testInFlightResponsesAfterCancelIgnored() {
        int numTargets = 3;
        ExchangeSink rootSink = mock(ExchangeSink.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildExec(numTargets, rootSink, listener, captured);

        task.run();
        assertEquals(numTargets, captured.size());

        task.cancel("cancelled");
        assertEquals(StageExecution.State.CANCELLED, task.getState());

        // Drive responses on all captured listeners — they should be ignored
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (StreamingResponseListener srl : captured) {
            srl.onStreamResponse(response, true);
        }

        // rootSink should NOT have been fed (responses were discarded)
        verify(rootSink, never()).feed(any());
        // listener.onFailure called exactly once (from cancel), no onResponse
        verify(listener, times(1)).onFailure(any());
        verify(listener, never()).onResponse(any());
    }

    /**
     * Cancel from CREATED state (before run): transitions to CANCELLED
     * and fires listener.
     *
     * Validates: Requirements 4.5
     */
    public void testCancelFromCreatedState() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildExec(numTargets, listener, captured);

        assertEquals(StageExecution.State.CREATED, task.getState());

        task.cancel("cancelled before run");

        assertEquals(StageExecution.State.CANCELLED, task.getState());
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertTrue(captor.getValue() instanceof TaskCancelledException);
        assertEquals("cancelled before run", captor.getValue().getMessage());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int initialBatchSize) {
        TerminationDecider decider = mock(TerminationDecider.class);
        when(decider.initialBatchSize(anyInt())).thenReturn(initialBatchSize);
        when(decider.shouldTerminate(any(), anyInt(), anyInt())).thenReturn(false);

        Stage stage = mock(Stage.class);
        when(stage.getTerminationDecider()).thenReturn(decider);
        when(stage.getStageId()).thenReturn(0);
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

    private FanOutStageExecution buildExec(int numTargets, ActionListener<Void> listener, List<StreamingResponseListener> captured) {
        QueryState state = new QueryState();
        Stage stage = mockStage(numTargets);
        return new FanOutStageExecution(
            stage,
            "test-query",
            buildTargets(numTargets),
            List.of(),
            Runnable::run,
            null,
            state.rootSink(),
            new SinkFeedingHandler(new SimpleExchangeSink()),
            state.completedStages(),
            state.shuffleManifests(),
            (request, node, streamListener) -> captured.add(streamListener),
            listener,
            new StageMetrics(stage.getStageId())
        );
    }

    private FanOutStageExecution buildExec(
        int numTargets,
        ExchangeSink rootSink,
        ActionListener<Void> listener,
        List<StreamingResponseListener> captured
    ) {
        QueryState state = new QueryState();
        Stage stage = mockStage(numTargets);
        return new FanOutStageExecution(
            stage,
            "test-query",
            buildTargets(numTargets),
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new SinkFeedingHandler(rootSink),
            state.completedStages(),
            state.shuffleManifests(),
            (request, node, streamListener) -> captured.add(streamListener),
            listener,
            new StageMetrics(stage.getStageId())
        );
    }
}
