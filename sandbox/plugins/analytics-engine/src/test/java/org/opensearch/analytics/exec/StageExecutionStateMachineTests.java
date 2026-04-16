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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * State-machine tests for {@link FanOutStageExecution}. These tests exercise
 * construction, {@code run()}, and the initial batch dispatch logic directly.
 *
 * <p>Tests drive completions via captured {@link StreamingResponseListener}
 * instances from the {@link ShardRequestClient}.
 *
 * Validates: Requirements 9.1, 9.2, 9.3, 9.4, 2.2, 2.3, 2.4
 */
@SuppressWarnings("unchecked")
public class StageExecutionStateMachineTests extends OpenSearchTestCase {

    /**
     * Helper that constructs a {@link FanOutStageExecution} from a {@link QueryExecutionContext}
     * facade, unpacking it into the explicit-deps constructor. Avoids repeating the
     * 12-arg constructor in every test.
     */
    /**
     * Test helper: constructs a {@link FanOutStageExecution} with sensible defaults.
     * Uses a fresh {@link QueryState} internally (tests that need to inspect
     * the state should use the overload below).
     */
    private static FanOutStageExecution newExec(
        Stage stage,
        List<ShardTarget> targets,
        List<FragmentExecutionRequest.PlanAlternative> plans,
        StageResultHandler handler,
        ShardRequestClient client,
        ActionListener<Void> listener
    ) {
        QueryState state = new QueryState();
        return new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            plans,
            Runnable::run,
            null,
            state.rootSink(),
            handler,
            state.completedStages(),
            state.shuffleManifests(),
            client,
            listener,
            new StageMetrics(stage.getStageId())
        );
    }

    /** Overload with explicit {@link QueryState} (for tests that inspect shared state). */
    private static FanOutStageExecution newExec(
        Stage stage,
        List<ShardTarget> targets,
        List<FragmentExecutionRequest.PlanAlternative> plans,
        QueryState state,
        StageResultHandler handler,
        ShardRequestClient client,
        ActionListener<Void> listener
    ) {
        return new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            plans,
            Runnable::run,
            null,
            state.rootSink(),
            handler,
            state.completedStages(),
            state.shuffleManifests(),
            client,
            listener,
            new StageMetrics(stage.getStageId())
        );
    }

    /** Overload with explicit parentTask (for cancellation tests). */
    private static FanOutStageExecution newExec(
        Stage stage,
        List<ShardTarget> targets,
        List<FragmentExecutionRequest.PlanAlternative> plans,
        Task parentTask,
        StageResultHandler handler,
        ShardRequestClient client,
        ActionListener<Void> listener
    ) {
        QueryState state = new QueryState();
        return new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            plans,
            Runnable::run,
            parentTask,
            state.rootSink(),
            handler,
            state.completedStages(),
            state.shuffleManifests(),
            client,
            listener,
            new StageMetrics(stage.getStageId())
        );
    }

    /**
     * After construction (before {@code run()} is called), the task must be in CREATED state.
     *
     * Validates: Requirements 9.1, 9.2
     */
    public void testInitialStateIsCreated() {
        Stage stage = mockStage(5);
        List<ShardTarget> targets = buildTargets(3);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        assertEquals("State must be CREATED before run() is called", StageExecution.State.CREATED, task.getState());
    }

    /**
     * When {@code run()} is called with zero targets, the task transitions directly
     * to SUCCEEDED, the listener is notified with success, and no submissions occur.
     *
     * Validates: Requirements 9.4, 2.3
     */
    public void testRunWithZeroTargetsGoesDirectlyToSucceeded() {
        Stage stage = mockStage(0);
        List<ShardTarget> targets = List.of();
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        assertEquals("State must be SUCCEEDED after run() with zero targets", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        assertEquals("No submissions should have been made", 0, captured.size());
    }

    /**
     * When {@code run()} is called with 3 targets and initialBatchSize=3, the task
     * transitions to RUNNING and exactly 3 submissions are captured.
     *
     * Validates: Requirements 9.3, 2.2
     */
    public void testRunWithNonEmptyBatchTransitionsToRunning() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        assertEquals("State must be RUNNING after run() with non-empty batch", StageExecution.State.RUNNING, task.getState());
        assertEquals("Submissions must equal number of targets", numTargets, captured.size());
    }

    /**
     * When {@code run()} is called with 5 targets and initialBatchSize=2, the task
     * transitions to RUNNING, only 2 submissions are captured, completedTasks is 0,
     * and inFlight is 2.
     *
     * Validates: Requirements 9.3, 2.2, 2.4
     */
    public void testRunWithInitialBatchSizeLessThanTotal() {
        int numTargets = 5;
        int initialBatch = 2;
        Stage stage = mockStage(initialBatch);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        assertEquals("State must be RUNNING", StageExecution.State.RUNNING, task.getState());
        assertEquals("Only initialBatchSize submissions should occur", initialBatch, captured.size());
        assertEquals("completedTasks must be 0 after run()", 0, task.getCompletedTasks());
        assertEquals("inFlight must equal initialBatchSize", initialBatch, task.getInFlight());
    }

    /**
     * When initialBatchSize (10) exceeds totalTargets (3), actualBatch is clamped
     * to totalTargets. Exactly 3 submissions are captured.
     *
     * Validates: Requirements 2.2
     */
    public void testInitialBatchSizeGreaterThanTotalClampsToTotal() {
        int numTargets = 3;
        int initialBatch = 10;
        Stage stage = mockStage(initialBatch);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        assertEquals("Submissions must be clamped to totalTargets when initialBatchSize exceeds it", numTargets, captured.size());
    }

    // ─── Phase 2: Response / failure state-transition tests ────────────

    /**
     * 3 targets, drive 3 final responses → final state == SUCCEEDED,
     * listener.onResponse called once, completedTasks == 3.
     *
     * Validates: Requirements 4.1, 9.6, 9.7
     */
    public void testAllSuccessfulResponsesEndInSucceeded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);
        Set<Integer> completedStages = ConcurrentHashMap.newKeySet();
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(rootSink), client, listener);

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 3 successful final responses
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onStreamResponse(response, true);
        }

        assertEquals("Final state must be SUCCEEDED", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        assertEquals("completedTasks must equal number of targets", numTargets, task.getCompletedTasks());
    }

    /**
     * 3 targets: drive 1 success + 1 failure + 1 success →
     * final state == FAILED, listener.onFailure called once with RuntimeException
     * whose message contains "Stage 0 failed".
     *
     * Validates: Requirements 4.2, 4.3, 5.1, 9.6, 9.7
     */
    public void testAnyFailureEndsInFailed() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);
        Set<Integer> completedStages = ConcurrentHashMap.newKeySet();
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(rootSink), client, listener);

        task.run();

        // 1 success, 1 failure, 1 success
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);
        RuntimeException cause = new RuntimeException("shard failed");
        captured.get(1).onFailure(cause);
        captured.get(2).onStreamResponse(response, true);

        assertEquals("Final state must be FAILED", StageExecution.State.FAILED, task.getState());
        verify(listener, never()).onResponse(null);
        org.mockito.ArgumentCaptor<Exception> captor = org.mockito.ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        Exception capturedEx = captor.getValue();
        assertTrue("Failure must be a RuntimeException", capturedEx instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", capturedEx.getMessage().contains("Stage 0 failed"));
        assertSame("Cause must be the original exception", cause, capturedEx.getCause());
    }

    /**
     * Drive 2 failures with different exceptions → the listener receives
     * the first exception as the cause, not the second.
     *
     * Validates: Requirements 5.1
     */
    public void testFirstFailureIsCapturedSubsequentFailuresDiscarded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        RuntimeException first = new RuntimeException("first failure");
        RuntimeException second = new RuntimeException("second failure");
        captured.get(0).onFailure(first);
        captured.get(1).onFailure(second);
        // Drive one more response to drain inFlight to 0
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("f"), Collections.singletonList(new Object[] { "v" }));
        captured.get(2).onStreamResponse(response, true);

        assertEquals("Final state must be FAILED", StageExecution.State.FAILED, task.getState());
        org.mockito.ArgumentCaptor<Exception> captor = org.mockito.ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertSame("Cause must be the FIRST exception", first, captor.getValue().getCause());
    }

    /**
     * When collectMetadata=false, response feeds the rootSink.
     *
     * Validates: Requirements 4.2
     */
    public void testResponseFeedsRootSinkWhenNotCollectingMetadata() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(rootSink), client, listener);

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        verify(rootSink, times(1)).feed(response);
    }

    /**
     * When collectMetadata=true, response stores the manifest in the manifests
     * map and does NOT feed the rootSink.
     *
     * Validates: Requirements 4.3
     */
    public void testResponseStoresManifestWhenCollectingMetadata() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        when(stage.isShuffleWrite()).thenReturn(true);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);
        QueryState state = new QueryState(rootSink);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), state, new ManifestCollectingHandler(), client, listener);

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(Map.of("0", "path/to/file"));
        captured.get(0).onStreamResponse(response, true);

        verify(rootSink, never()).feed(org.mockito.ArgumentMatchers.any());
        assertFalse("Manifests map should not be empty after metadata response", state.shuffleManifests().isEmpty());
    }

    /**
     * Drive 2 successes + 1 failure → metrics.tasksCompleted == 2,
     * metrics.tasksFailed == 1.
     *
     * Validates: Requirements 4.1, 5.1
     */
    public void testMetricsIncrementedOnSuccessAndFailure() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);
        captured.get(1).onStreamResponse(response, true);
        captured.get(2).onFailure(new RuntimeException("oops"));

        assertEquals("tasksCompleted must be 2", 2, task.getMetrics().getTasksCompleted());
        assertEquals("tasksFailed must be 1", 1, task.getMetrics().getTasksFailed());
    }

    // ─── Phase 2: Early termination and sliding-window semantics ──────

    /**
     * Mock decider returns true after 1st completion → after 1st response,
     * finishStageInternal is called immediately, state == SUCCEEDED, listener
     * signaled with onResponse. Late in-flight responses are discarded.
     *
     * Validates: Requirements 4.4, 4.5, 9.5
     */
    public void testDeciderTerminationFinishesImmediately() {
        int numTargets = 2;
        int initialBatch = 2;
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 1st completion — decider says terminate → finishStageInternal called immediately
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        assertEquals("State must be SUCCEEDED after decider triggers", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Drive to SUCCEEDED via 3 responses; send a 4th response →
     * state unchanged (SUCCEEDED), listener.onResponse called only once.
     *
     * Validates: Requirements 4.4, 9.9
     */
    public void testLateResponseAfterTerminalIsDiscarded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(rootSink), client, listener);

        task.run();

        // Drive 3 responses → SUCCEEDED
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onStreamResponse(response, true);
        }
        assertEquals(StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);

        // 4th late response on the last listener — should be discarded (isTerminated check)
        captured.get(2).onStreamResponse(response, true);

        assertEquals("State must remain SUCCEEDED after late response", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
    }

    /**
     * Drive to SUCCEEDED via 3 responses; call onFailure → state unchanged
     * (SUCCEEDED), listener.onFailure not called.
     *
     * Validates: Requirements 4.5, 9.9
     */
    public void testLateFailureAfterTerminalIsDiscarded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();

        // Drive 3 responses → SUCCEEDED
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onStreamResponse(response, true);
        }
        assertEquals(StageExecution.State.SUCCEEDED, task.getState());

        // Late failure — should be discarded (onFailure still calls onTaskCompletion
        // but finishStageInternal won't transition from SUCCEEDED)
        captured.get(2).onFailure(new RuntimeException("late failure"));

        assertEquals("State must remain SUCCEEDED after late failure", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    /**
     * 5 targets, initialBatchSize=2, drive 1 response → client received
     * 3 calls total (2 initial + 1 follow-up dispatch).
     *
     * Validates: Requirements 4.6, 9.5
     */
    public void testSubmitsNextAfterCompletionWhenNotTerminated() {
        int numTargets = 5;
        int initialBatch = 2;
        Stage stage = mockStage(initialBatch);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();
        assertEquals("Initial submissions must be 2", 2, captured.size());

        // Drive 1 response for targets.get(0) → should dispatch next target (index 2)
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        assertEquals("Client must have received 3 calls (2 initial + 1 follow-up)", 3, captured.size());
    }

    /**
     * 10 targets, initialBatchSize=2, decider terminates after 1st completion →
     * client received only 2 calls total (no follow-up dispatch).
     *
     * Validates: Requirements 4.4, 9.5
     */
    public void testEarlyTerminationPreventsFurtherDispatch() {
        int numTargets = 10;
        int initialBatch = 2;
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();
        assertEquals("Initial submissions must be 2", 2, captured.size());

        // Drive 1 response → decider terminates, no follow-up dispatch
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        assertEquals("Client must have received only 2 calls (no follow-up after termination)", 2, captured.size());
    }

    /**
     * 2 targets in flight (initialBatchSize=2, totalTargets=2), decider terminates
     * on 1st completion → finishStageInternal called immediately, state == SUCCEEDED,
     * listener signaled. 2nd response is discarded (late arrival after termination).
     *
     * Validates: Requirements 4.6, 9.5, 9.9
     */
    public void testInFlightDrainAfterTermination() {
        int numTargets = 2;
        int initialBatch = 2;
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 1st completion → TERMINATED → finishStageInternal → SUCCEEDED, listener called
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        assertEquals("State must be SUCCEEDED after termination finishes immediately", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);

        // Drive 2nd completion → late, discarded (state is already SUCCEEDED)
        captured.get(1).onStreamResponse(response, true);

        assertEquals("State must remain SUCCEEDED after late response", StageExecution.State.SUCCEEDED, task.getState());
        // listener.onResponse still called only once
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    // ─── Phase 5: Concurrency and late-arrival tests ─────────────────────

    /**
     * 10 targets, initialBatchSize=10. Spawn 10 threads, each calling
     * onStreamResponse with isLast=true. Assert: final state == SUCCEEDED,
     * listener.onResponse called exactly once, completedTasks == 10.
     *
     * Validates: Requirements 9.7, 9.10
     */
    public void testConcurrentCompletionsSerializeCorrectly() throws Exception {
        int numTargets = 10;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        AtomicInteger listenerCalls = new AtomicInteger(0);
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                listenerCalls.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalls.incrementAndGet();
            }
        };

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);
        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numTargets);
        Thread[] threads = new Thread[numTargets];
        for (int i = 0; i < numTargets; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                captured.get(idx).onStreamResponse(response, true);
                doneLatch.countDown();
            });
            threads[i].start();
        }
        startLatch.countDown();
        doneLatch.await();

        assertEquals(StageExecution.State.SUCCEEDED, task.getState());
        assertEquals(1, listenerCalls.get());
        assertEquals(numTargets, task.getCompletedTasks());
    }

    /**
     * 10 targets, initialBatchSize=10. Spawn 10 threads — thread 0 calls
     * onFailure, threads 1–9 call onStreamResponse. Assert: final state ==
     * FAILED, listener called exactly once.
     *
     * Validates: Requirements 9.7, 9.10
     */
    public void testConcurrentCompletionsWithOneFailureEndsFailed() throws Exception {
        int numTargets = 10;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        AtomicInteger listenerCalls = new AtomicInteger(0);
        AtomicInteger failureCalls = new AtomicInteger(0);
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                listenerCalls.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalls.incrementAndGet();
                failureCalls.incrementAndGet();
            }
        };

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);
        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numTargets);
        Thread[] threads = new Thread[numTargets];
        for (int i = 0; i < numTargets; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (idx == 0) {
                    captured.get(idx).onFailure(new RuntimeException("shard failed"));
                } else {
                    captured.get(idx).onStreamResponse(response, true);
                }
                doneLatch.countDown();
            });
            threads[i].start();
        }
        startLatch.countDown();
        doneLatch.await();

        assertEquals(StageExecution.State.FAILED, task.getState());
        assertEquals(1, listenerCalls.get());
        assertEquals(1, failureCalls.get());
    }

    /**
     * 10 targets, initialBatchSize=3, decider returns true when completedTasks >= 2.
     * Spawn 3 threads for the initial batch concurrently. Assert: state is a terminal
     * state (SUCCEEDED or FAILED), client received a bounded number of calls
     * (no over-dispatch).
     *
     * Validates: Requirements 9.7, 9.10
     */
    public void testConcurrentEarlyTerminationRace() throws Exception {
        int numTargets = 10;
        int initialBatch = 3;
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 2;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        AtomicInteger listenerCalls = new AtomicInteger(0);
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                listenerCalls.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalls.incrementAndGet();
            }
        };

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);
        task.run();
        assertEquals(initialBatch, captured.size());

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(initialBatch);
        Thread[] threads = new Thread[initialBatch];
        for (int i = 0; i < initialBatch; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                captured.get(idx).onStreamResponse(response, true);
                doneLatch.countDown();
            });
            threads[i].start();
        }
        startLatch.countDown();
        doneLatch.await();

        // State must be terminal
        StageExecution.State finalState = task.getState();
        assertTrue(
            "State must be terminal (SUCCEEDED or FAILED), was " + finalState,
            finalState == StageExecution.State.SUCCEEDED || finalState == StageExecution.State.FAILED
        );
        // Listener called exactly once
        assertEquals(1, listenerCalls.get());
        // Client calls bounded: initial batch (3) + at most 1 follow-up per completion before termination
        assertTrue("Client calls must be bounded, was " + captured.size(), captured.size() <= initialBatch + initialBatch);
    }

    /**
     * 5 targets, initialBatchSize=2, decider returns true on 1st completion.
     * Drive 1 response → verify client received exactly 2 calls
     * (no 3rd dispatch after termination).
     *
     * Validates: Requirements 9.7, 9.10
     */
    public void testRaceBetweenTerminationAndNewDispatch() {
        int numTargets = 5;
        int initialBatch = 2;
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        AtomicInteger listenerCalls = new AtomicInteger(0);
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                listenerCalls.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalls.incrementAndGet();
            }
        };

        List<StreamingResponseListener> captured = new ArrayList<>();
        ShardRequestClient client = capturingClient(captured);

        FanOutStageExecution task = newExec(stage, targets, List.of(), new SinkFeedingHandler(new SimpleExchangeSink()), client, listener);
        task.run();
        assertEquals("Initial submissions must be 2", 2, captured.size());

        // Drive 1st completion — decider terminates, no follow-up dispatch
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        captured.get(0).onStreamResponse(response, true);

        assertEquals("Client must have received exactly 2 calls (no 3rd dispatch after termination)", 2, captured.size());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    /**
     * Build a mock {@link Stage} whose {@link TerminationDecider} returns the
     * given {@code initialBatchSize} and never terminates early.
     */
    private Stage mockStage(int initialBatchSize) {
        TerminationDecider decider = mock(TerminationDecider.class);
        when(decider.initialBatchSize(org.mockito.ArgumentMatchers.anyInt())).thenReturn(initialBatchSize);
        when(
            decider.shouldTerminate(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
            )
        ).thenReturn(false);

        Stage stage = mock(Stage.class);
        when(stage.getTerminationDecider()).thenReturn(decider);
        when(stage.getStageId()).thenReturn(0);
        when(stage.isShuffleWrite()).thenReturn(false);
        return stage;
    }

    /**
     * Build a mock {@link Stage} with a custom {@link TerminationDecider}.
     */
    private Stage mockStageWithDecider(int initialBatchSize, TerminationDecider decider) {
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
     * Create a {@link ShardRequestClient} that captures {@link StreamingResponseListener}
     * instances. The returned client does NOT call the listener —
     * tasks remain "in flight" until the test drives completions manually.
     */
    private ShardRequestClient capturingClient(List<StreamingResponseListener> captured) {
        return (request, node, listener) -> captured.add(listener);
    }
}
