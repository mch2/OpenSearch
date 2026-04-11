/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
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
 * State-machine tests for {@link StageExecution}. These tests exercise
 * construction, {@code run()}, and the initial batch dispatch logic directly.
 *
 * <p>These tests will NOT compile until Phase 2 produces the {@code StageExecution} class.
 *
 * Validates: Requirements 9.1, 9.2, 9.3, 9.4, 2.2, 2.3, 2.4
 */
@SuppressWarnings("unchecked")
public class StageExecutionStateMachineTests extends OpenSearchTestCase {

    /**
     * After construction (before {@code run()} is called), the task must be in CREATED state.
     *
     * Validates: Requirements 9.1, 9.2
     */
    public void testInitialStateIsCreated() {
        Stage stage = mockStage(5);
        List<ShardTarget> targets = buildTargets(3);
        ActionListener<Void> listener = mock(ActionListener.class);
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = capturingSubmitter(submissions);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            submitter,
            listener
        );

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
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = capturingSubmitter(submissions);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            submitter,
            listener
        );

        task.run();

        assertEquals("State must be SUCCEEDED after run() with zero targets", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        assertEquals("No submissions should have been made", 0, submissions.get());
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
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = capturingSubmitter(submissions);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            submitter,
            listener
        );

        task.run();

        assertEquals("State must be RUNNING after run() with non-empty batch", StageExecution.State.RUNNING, task.getState());
        assertEquals("Submissions must equal number of targets", numTargets, submissions.get());
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
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = capturingSubmitter(submissions);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            submitter,
            listener
        );

        task.run();

        assertEquals("State must be RUNNING", StageExecution.State.RUNNING, task.getState());
        assertEquals("Only initialBatchSize submissions should occur", initialBatch, submissions.get());
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
        AtomicInteger submissions = new AtomicInteger(0);
        TaskSubmitter submitter = capturingSubmitter(submissions);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            submitter,
            listener
        );

        task.run();

        assertEquals("Submissions must be clamped to totalTargets when initialBatchSize exceeds it", numTargets, submissions.get());
    }

    // ─── Phase 2: Response / failure state-transition tests ────────────

    /**
     * 3 targets, drive 3 handleResponse calls → final state == SUCCEEDED,
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, rootSink, completedStages, shuffleManifests, null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 3 successful responses
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            task.handleResponse(response, targets.get(i));
        }

        assertEquals("Final state must be SUCCEEDED", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
        assertEquals("completedTasks must equal number of targets", numTargets, task.getCompletedTasks());
    }

    /**
     * 3 targets: drive 1 handleResponse + 1 handleFailure + 1 handleResponse →
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, rootSink, completedStages, shuffleManifests, null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        // 1 success, 1 failure, 1 success
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));
        RuntimeException cause = new RuntimeException("shard failed");
        task.handleFailure(cause);
        task.handleResponse(response, targets.get(2));

        assertEquals("Final state must be FAILED", StageExecution.State.FAILED, task.getState());
        verify(listener, never()).onResponse(null);
        org.mockito.ArgumentCaptor<Exception> captor = org.mockito.ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        Exception captured = captor.getValue();
        assertTrue("Failure must be a RuntimeException", captured instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", captured.getMessage().contains("Stage 0 failed"));
        assertSame("Cause must be the original exception", cause, captured.getCause());
    }

    /**
     * Drive 2 handleFailure calls with different exceptions → the listener receives
     * the first exception as the cause, not the second.
     *
     * Validates: Requirements 5.1
     */
    public void testFirstFailureIsCapturedSubsequentFailuresDiscarded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        RuntimeException first = new RuntimeException("first failure");
        RuntimeException second = new RuntimeException("second failure");
        task.handleFailure(first);
        task.handleFailure(second);
        // Drive one more response to drain inFlight to 0
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("f"), Collections.singletonList(new Object[] { "v" }));
        task.handleResponse(response, targets.get(2));

        assertEquals("Final state must be FAILED", StageExecution.State.FAILED, task.getState());
        org.mockito.ArgumentCaptor<Exception> captor = org.mockito.ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertSame("Cause must be the FIRST exception", first, captor.getValue().getCause());
    }

    /**
     * When collectMetadata=false, handleResponse feeds the rootSink with the response.
     *
     * Validates: Requirements 4.2
     */
    public void testResponseFeedsRootSinkWhenNotCollectingMetadata() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, rootSink, ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        verify(rootSink, times(1)).feed(response);
    }

    /**
     * When collectMetadata=true, handleResponse stores the manifest in the manifests
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
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, rootSink, ConcurrentHashMap.newKeySet(), shuffleManifests, null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(Map.of("0", "path/to/file"));
        task.handleResponse(response, targets.get(0));

        verify(rootSink, never()).feed(org.mockito.ArgumentMatchers.any());
        assertFalse("Manifests map should not be empty after metadata response", shuffleManifests.isEmpty());
    }

    /**
     * Drive 2 handleResponse + 1 handleFailure → metrics.tasksCompleted == 2,
     * metrics.tasksFailed == 1.
     *
     * Validates: Requirements 4.1, 5.1
     */
    public void testMetricsIncrementedOnSuccessAndFailure() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));
        task.handleResponse(response, targets.get(1));
        task.handleFailure(new RuntimeException("oops"));

        assertEquals("tasksCompleted must be 2", 2, task.getMetrics().getTasksCompleted());
        assertEquals("tasksFailed must be 1", 1, task.getMetrics().getTasksFailed());
    }

    // ─── Phase 2: Early termination and sliding-window semantics ──────

    /**
     * Mock decider returns true after 1st completion → after 1st handleResponse,
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
        AtomicInteger submissions = new AtomicInteger(0);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(submissions),
            listener
        );

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 1st completion — decider says terminate → finishStageInternal called immediately
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        assertEquals("State must be SUCCEEDED after decider triggers", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Drive to SUCCEEDED via 3 responses; call handleResponse a 4th time →
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, rootSink, ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        // Drive 3 responses → SUCCEEDED
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            task.handleResponse(response, targets.get(i));
        }
        assertEquals(StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);

        // 4th late response — should be discarded
        ShardTarget extraTarget = buildTargets(4).get(3);
        task.handleResponse(response, extraTarget);

        assertEquals("State must remain SUCCEEDED after late response", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);
    }

    /**
     * Drive to SUCCEEDED via 3 responses; call handleFailure → state unchanged
     * (SUCCEEDED), listener.onFailure not called.
     *
     * Validates: Requirements 4.5, 9.9
     */
    public void testLateFailureAfterTerminalIsDiscarded() {
        int numTargets = 3;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> listener = mock(ActionListener.class);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();

        // Drive 3 responses → SUCCEEDED
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        for (int i = 0; i < numTargets; i++) {
            task.handleResponse(response, targets.get(i));
        }
        assertEquals(StageExecution.State.SUCCEEDED, task.getState());

        // Late failure — should be discarded
        task.handleFailure(new RuntimeException("late failure"));

        assertEquals("State must remain SUCCEEDED after late failure", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    /**
     * 5 targets, initialBatchSize=2, drive 1 handleResponse → submitter received
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
        AtomicInteger submissions = new AtomicInteger(0);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(submissions),
            listener
        );

        task.run();
        assertEquals("Initial submissions must be 2", 2, submissions.get());

        // Drive 1 handleResponse for targets.get(0) → should dispatch next target (index 2)
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        assertEquals("Submitter must have received 3 calls (2 initial + 1 follow-up)", 3, submissions.get());
    }

    /**
     * 10 targets, initialBatchSize=2, decider terminates after 1st completion →
     * submitter received only 2 calls total (no follow-up dispatch).
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
        AtomicInteger submissions = new AtomicInteger(0);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(submissions),
            listener
        );

        task.run();
        assertEquals("Initial submissions must be 2", 2, submissions.get());

        // Drive 1 handleResponse → decider terminates, no follow-up dispatch
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        assertEquals("Submitter must have received only 2 calls (no follow-up after termination)", 2, submissions.get());
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );

        task.run();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        // Drive 1st completion → TERMINATED → finishStageInternal → SUCCEEDED, listener called
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        assertEquals("State must be SUCCEEDED after termination finishes immediately", StageExecution.State.SUCCEEDED, task.getState());
        verify(listener, times(1)).onResponse(null);

        // Drive 2nd completion → late, discarded (state is already SUCCEEDED)
        task.handleResponse(response, targets.get(1));

        assertEquals("State must remain SUCCEEDED after late response", StageExecution.State.SUCCEEDED, task.getState());
        // listener.onResponse still called only once
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    // ─── Phase 5: Concurrency and late-arrival tests ─────────────────────

    /**
     * 10 targets, initialBatchSize=10. Spawn 10 threads, each calling
     * handleResponse with a distinct target. Assert: final state == SUCCEEDED,
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );
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
                task.handleResponse(response, targets.get(idx));
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
     * handleFailure, threads 1–9 call handleResponse. Assert: final state ==
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

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(new AtomicInteger(0)),
            listener
        );
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
                    task.handleFailure(new RuntimeException("shard failed"));
                } else {
                    task.handleResponse(response, targets.get(idx));
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
     * state (SUCCEEDED or FAILED), submitter received a bounded number of calls
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
        AtomicInteger submissions = new AtomicInteger(0);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(submissions),
            listener
        );
        task.run();
        assertEquals(initialBatch, submissions.get());

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
                task.handleResponse(response, targets.get(idx));
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
        // Submitter calls bounded: initial batch (3) + at most 1 follow-up per completion before termination
        assertTrue("Submitter calls must be bounded, was " + submissions.get(), submissions.get() <= initialBatch + initialBatch);
    }

    /**
     * 5 targets, initialBatchSize=2, decider returns true on 1st completion.
     * Drive 1 handleResponse → verify submitter received exactly 2 calls
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
        AtomicInteger submissions = new AtomicInteger(0);

        StageExecution task = new StageExecution(
            stage,
            targets,
            List.of(),
            new QueryExecutionContext("test-query", Runnable::run, mock(ExchangeSink.class), ConcurrentHashMap.newKeySet(), new ConcurrentHashMap<>(), null),
            capturingSubmitter(submissions),
            listener
        );
        task.run();
        assertEquals("Initial submissions must be 2", 2, submissions.get());

        // Drive 1st completion — decider terminates, no follow-up dispatch
        FragmentExecutionResponse response = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        task.handleResponse(response, targets.get(0));

        assertEquals("Submitter must have received exactly 2 calls (no 3rd dispatch after termination)", 2, submissions.get());
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
        when(stage.isCoordinatorGather()).thenReturn(false);
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
        when(stage.isCoordinatorGather()).thenReturn(false);
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
     * Create a {@link TaskSubmitter} that counts submissions without actually
     * dispatching anything. The returned submitter does NOT call the listener —
     * tasks remain "in flight" until the test drives completions manually.
     */
    private TaskSubmitter capturingSubmitter(AtomicInteger counter) {
        return (request, node, listener) -> counter.incrementAndGet();
    }
}
