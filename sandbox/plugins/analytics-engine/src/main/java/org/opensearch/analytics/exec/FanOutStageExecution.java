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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-stage dispatch unit of work for fan-out to data nodes. Owns state
 * for a single {@link StageExecutor#dispatch} invocation. Not a Task in
 * the OpenSearch framework sense.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → TERMINATED → SUCCEEDED | FAILED}
 *
 * <p>Instances are one-shot: constructed, {@code run()} called once,
 * listener signaled once, discarded.
 *
 * <p>Takes explicit dependencies (not a context bag) — minimal knowledge
 * principle. Only has access to what it directly needs.
 *
 * @opensearch.internal
 */
final class FanOutStageExecution implements StageExecution {

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    // Manage outbound tasks for early termination
    private final ConcurrentLinkedQueue<ShardTarget> pendingTargets;
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    // Immutable config (explicit deps)
    private final Stage stage;
    private final String queryId;
    private final List<ShardTarget> targets;
    private final List<FragmentExecutionRequest.PlanAlternative> planAlternatives;
    private final Executor searchExecutor;
    private final Task parentTask;
    private final ExchangeSink rootSink;  // for TerminationDecider inspection
    private final StageResultHandler resultHandler;
    private final ShardRequestClient client;
    private final ActionListener<Void> listener;

    // Shared query state (references to mutable collections in QueryState)
    private final Set<Integer> completedStages;
    private final Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests;

    // Per-dispatch state (created internally)
    private final StageMetrics metrics;

    FanOutStageExecution(
        Stage stage,
        String queryId,
        List<ShardTarget> targets,
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives,
        Executor searchExecutor,
        Task parentTask,
        ExchangeSink rootSink,
        StageResultHandler resultHandler,
        Set<Integer> completedStages,
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests,
        ShardRequestClient client,
        ActionListener<Void> listener,
        StageMetrics metrics
    ) {
        this.stage = stage;
        this.queryId = queryId;
        this.targets = targets;
        this.pendingTargets = new ConcurrentLinkedQueue<>(targets);
        this.planAlternatives = planAlternatives;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.rootSink = rootSink;
        this.resultHandler = resultHandler;
        this.completedStages = completedStages;
        this.shuffleManifests = shuffleManifests;
        this.client = client;
        this.listener = listener;
        this.metrics = metrics;
    }

    // ─── Entry point ────────────────────────────────────────────────────
    void run() {
        metrics.recordStart();
        int initialDispatchCount = Math.min(stage.getTerminationDecider().initialBatchSize(targets.size()), targets.size());
        if (initialDispatchCount == 0) {
            transitionTo(State.CREATED, State.SUCCEEDED);
            metrics.recordEnd();
            completedStages.add(stage.getStageId());
            listener.onResponse(null);
            return;
        }
        transitionTo(State.CREATED, State.RUNNING);
        for (int i = 0; i < initialDispatchCount; i++) {
            ShardTarget target = pendingTargets.poll();
            if (target == null) break;  // re-entrant completion already drained the queue
            inFlight.incrementAndGet();
            dispatchShardTask(target);
        }
    }

    private void dispatchShardTask(ShardTarget target) {
        FragmentExecutionRequest request = new FragmentExecutionRequest(queryId, stage.getStageId(), target.shardId(), planAlternatives);
        client.send(request, target.node(), new StreamingResponseListener() {
            @Override
            public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
                searchExecutor.execute(() -> {
                    if (isTerminated()) return;

                    // Process batch via the stage's result handler
                    resultHandler.onBatch(response, target);

                    // Completion logic only on final response
                    if (isLast) {
                        metrics.incrementTasksCompleted();
                        onTaskCompletion();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                failure.compareAndSet(null, e);
                metrics.incrementTasksFailed();
                onTaskCompletion();
            }
        });
    }

    private void onTaskCompletion() {
        int completed = completedTasks.incrementAndGet();

        // Check termination — if decider says stop, finish immediately
        if (state.get() == State.RUNNING) {
            if (stage.getTerminationDecider().shouldTerminate(rootSink, completed, targets.size())) {
                state.compareAndSet(State.RUNNING, State.TERMINATED);
                finishStageInternal();
                return;
            }
        }

        // Dispatch next pending target if still running
        if (state.get() == State.RUNNING) {
            ShardTarget next = pendingTargets.poll();
            if (next != null) {
                inFlight.incrementAndGet();
                dispatchShardTask(next);
            }
        }

        // Normal completion: all in-flight tasks drained
        if (inFlight.decrementAndGet() == 0) {
            finishStageInternal();
        }
    }

    void finishStageInternal() {
        Exception captured = failure.get();
        State terminal = captured != null ? State.FAILED : State.SUCCEEDED;
        if (!transitionToTerminal(terminal)) {
            return;  // another thread already finalized
        }
        metrics.recordEnd();
        if (captured != null) {
            if (parentTask instanceof CancellableTask ct && ct.isCancelled()) {
                listener.onFailure(new TaskCancelledException("query cancelled"));
            } else {
                listener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", captured));
            }
        } else {
            if (resultHandler instanceof ManifestCollectingHandler mch) {
                shuffleManifests.put(stage.getStageId(), mch.getManifests());
            }
            completedStages.add(stage.getStageId());
            listener.onResponse(null);
        }
    }

    // ─── cancel ────────────────────────────────────────────────────────
    @Override
    public void cancel(String reason) {
        if (state.compareAndSet(State.CREATED, State.CANCELLED)
            || state.compareAndSet(State.RUNNING, State.CANCELLED)
            || state.compareAndSet(State.TERMINATED, State.CANCELLED)) {

            pendingTargets.clear();   // stop dispatching new targets
            // In-flight responses arriving after this point will see isTerminated() and noop.
            listener.onFailure(new TaskCancelledException(reason));
        }
        // else: already in a terminal state; no-op (idempotent)
    }

    // ─── StageExecution interface ───────────────────────────────────────
    @Override
    public int getStageId() {
        return stage.getStageId();
    }

    // ─── State machine helpers ──────────────────────────────────────────
    private void transitionTo(State expected, State next) {
        boolean ok = state.compareAndSet(expected, next);
        assert ok : "illegal state transition: expected " + expected + ", was " + state.get();
    }

    private boolean transitionToTerminal(State terminal) {
        return state.compareAndSet(State.TERMINATED, terminal) || state.compareAndSet(State.RUNNING, terminal);
    }

    private boolean isTerminated() {
        State s = state.get();
        return s == State.TERMINATED || s == State.SUCCEEDED || s == State.FAILED || s == State.CANCELLED;
    }

    // ─── Test accessors (package-private) ───────────────────────────────
    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public StageMetrics getMetrics() {
        return metrics;
    }

    int getCompletedTasks() {
        return completedTasks.get();
    }

    int getInFlight() {
        return inFlight.get();
    }
}
