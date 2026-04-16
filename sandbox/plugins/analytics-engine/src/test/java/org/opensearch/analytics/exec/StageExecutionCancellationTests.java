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
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
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
 * Cancellation-aware tests for {@link FanOutStageExecution}. These tests exercise
 * the cancellation logic in {@code finishStageInternal()} by constructing
 * FanOutStageExecution with a {@code parentTask} parameter and driving
 * completions via captured {@link StreamingResponseListener} instances.
 *
 * Validates: Requirements 1.2, 1.3, 1.5, 1.6, 5.4
 */
@SuppressWarnings("unchecked")
public class StageExecutionCancellationTests extends OpenSearchTestCase {

    /**
     * Bottom-up cancellation: parentTask is NOT cancelled, but one shard returns
     * TaskCancelledException. The exception should be wrapped as "Stage 0 failed".
     *
     * Validates: Requirements 1.2, 1.3, 1.6
     */
    public void testDataNodeCancellationWrappedAsStageFailure() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        CancellableTask parentTask = mockParentTask(false);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, parentTask, captured);
        task.run();

        // First target fails with TaskCancelledException
        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));

        // Other two succeed
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(1).onStreamResponse(response, true);
        captured.get(2).onStreamResponse(response, true);

        // Verify listener.onFailure called with RuntimeException wrapping
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        verify(listener, never()).onResponse(any());

        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a RuntimeException", capturedEx instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", capturedEx.getMessage().contains("Stage 0 failed"));

        // Verify metrics
        assertEquals("tasksFailed must be 1", 1, task.getMetrics().getTasksFailed());
        assertEquals("tasksCompleted must be 2", 2, task.getMetrics().getTasksCompleted());
    }

    /**
     * Top-down cancellation: parentTask IS cancelled, all targets return
     * TaskCancelledException. The listener should get a bare TaskCancelledException
     * with message "query cancelled" (not wrapped).
     *
     * Validates: Requirements 1.1, 1.3
     */
    public void testTopDownCancellationReturnsCleanException() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        CancellableTask parentTask = mockParentTask(true);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, parentTask, captured);
        task.run();

        // All 3 targets fail with TaskCancelledException
        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onFailure(new TaskCancelledException("task cancelled"));
        }

        // Verify listener.onFailure called with bare TaskCancelledException
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        verify(listener, never()).onResponse(any());

        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a TaskCancelledException", capturedEx instanceof TaskCancelledException);
        assertEquals("Message must be 'query cancelled'", "query cancelled", capturedEx.getMessage());

        // Verify metrics still recorded end time
        assertTrue("End time must be recorded", task.getMetrics().getEndTimeMs() > 0);
    }

    /**
     * In-flight drain: parentTask IS cancelled, 3 in-flight tasks. Listener must
     * NOT be signaled until all 3 have completed.
     *
     * Validates: Requirements 1.5
     */
    public void testCancellationWaitsForInFlightDrain() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        CancellableTask parentTask = mockParentTask(true);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, parentTask, captured);
        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        // First fails with cancellation → listener NOT signaled yet
        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));
        verify(listener, never()).onFailure(any());
        verify(listener, never()).onResponse(any());

        // Second completes → still not signaled
        captured.get(1).onStreamResponse(response, true);
        verify(listener, never()).onFailure(any());
        verify(listener, never()).onResponse(any());

        // Third completes → NOW signaled with TaskCancelledException
        captured.get(2).onStreamResponse(response, true);
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());

        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a TaskCancelledException", capturedEx instanceof TaskCancelledException);
        assertEquals("Message must be 'query cancelled'", "query cancelled", capturedEx.getMessage());
    }

    /**
     * Mixed failure and cancellation: parentTask IS cancelled, first captured
     * exception is a normal RuntimeException, second is TaskCancelledException.
     * Parent task state wins — listener gets bare TaskCancelledException.
     *
     * Validates: Requirements 1.3
     */
    public void testMixedFailureAndCancellationHonorsParentTaskState() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        CancellableTask parentTask = mockParentTask(true);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, parentTask, captured);
        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        // First captured exception is a normal RuntimeException
        captured.get(0).onFailure(new RuntimeException("shard OOM"));
        // Second is a TaskCancelledException
        captured.get(1).onFailure(new TaskCancelledException("task cancelled"));
        // Third completes normally to drain in-flight
        captured.get(2).onStreamResponse(response, true);

        // Parent task state wins: listener gets bare TaskCancelledException
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        verify(listener, never()).onResponse(any());

        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a TaskCancelledException", capturedEx instanceof TaskCancelledException);
        assertEquals("Message must be 'query cancelled'", "query cancelled", capturedEx.getMessage());
    }

    /**
     * Metrics: all 3 targets return TaskCancelledException. tasksFailed must be 3,
     * tasksCompleted must be 0.
     *
     * Validates: Requirements 1.6
     */
    public void testMetricsCountCancellationAsFailure() {
        int numTargets = 3;
        ActionListener<Void> listener = mock(ActionListener.class);
        CancellableTask parentTask = mockParentTask(false);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, parentTask, captured);
        task.run();

        // All 3 targets fail with TaskCancelledException
        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onFailure(new TaskCancelledException("task cancelled"));
        }

        assertEquals("tasksFailed must be 3", 3, task.getMetrics().getTasksFailed());
        assertEquals("tasksCompleted must be 0", 0, task.getMetrics().getTasksCompleted());
    }

    /**
     * Null parentTask fallback: when parentTask is null, cancellation exception
     * is wrapped as "Stage 0 failed" (existing behavior).
     *
     * Validates: Requirements 5.4
     */
    public void testNullParentTaskFallsBackToWrapping() {
        int numTargets = 1;
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        FanOutStageExecution task = buildStageExec(numTargets, listener, null, captured);
        task.run();

        // Target returns TaskCancelledException
        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));

        // Verify listener.onFailure called with RuntimeException wrapping
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        verify(listener, never()).onResponse(any());

        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a RuntimeException", capturedEx instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", capturedEx.getMessage().contains("Stage 0 failed"));
        assertTrue("Cause must be the TaskCancelledException", capturedEx.getCause() instanceof TaskCancelledException);
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    /**
     * Build a mock {@link Stage} whose {@link TerminationDecider} returns the
     * given {@code initialBatchSize} and never terminates early.
     */
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

    /**
     * Build a list of {@code count} target shards with distinct shard IDs and mock nodes.
     */
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

    /**
     * Create a mock {@link CancellableTask} with a controllable {@code isCancelled()} return value.
     */
    private CancellableTask mockParentTask(boolean cancelled) {
        CancellableTask parentTask = mock(CancellableTask.class);
        when(parentTask.isCancelled()).thenReturn(cancelled);
        return parentTask;
    }

    /**
     * Build a {@link StageExecution} with the given parameters, using {@code mockStage(numTargets)},
     * {@code buildTargets(numTargets)}, and a capturing client.
     */
    private FanOutStageExecution buildStageExec(
        int numTargets,
        ActionListener<Void> listener,
        Task parentTask,
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
            parentTask,
            state.rootSink(),
            new SinkFeedingHandler(new SimpleExchangeSink()),
            state.completedStages(),
            state.shuffleManifests(),
            (request, node, streamListener) -> captured.add(streamListener),
            listener,
            new StageMetrics(stage.getStageId())
        );
    }
}
