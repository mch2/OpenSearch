/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-stage dispatch unit of work. Owns state for a single
 * {@link StageExecutor#dispatch} invocation. Not a Task in the OpenSearch
 * framework sense.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → TERMINATED → SUCCEEDED | FAILED}
 *
 * <p>Instances are one-shot: constructed, {@code run()} called once, listener
 * signaled once, discarded.
 *
 * @opensearch.internal
 */
final class StageExecution {

    enum State {
        CREATED,
        RUNNING,
        TERMINATED,
        SUCCEEDED,
        FAILED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    // Manage outbound tasks for early termination
    private final ConcurrentLinkedQueue<ShardTarget> pendingTargets;
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private final Stage stage;
    private final List<ShardTarget> targets;
    private final List<FragmentExecutionRequest.PlanAlternative> planAlternatives;
    private final TaskSubmitter submitter;
    private final ActionListener<Void> listener;
    private final QueryExecutionContext context;

    // ─── Per-dispatch state (created internally) ─────────────────────────
    private final Map<ShardId, Map<Integer, String>> manifests;
    private final StageMetrics metrics;

    StageExecution(
        Stage stage,
        List<ShardTarget> targets,
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives,
        QueryExecutionContext context,
        TaskSubmitter submitter,
        ActionListener<Void> listener
    ) {
        this.stage = stage;
        this.targets = targets;
        this.pendingTargets = new ConcurrentLinkedQueue<>(targets);
        this.planAlternatives = planAlternatives;
        this.context = context;
        this.submitter = submitter;
        this.listener = listener;
        this.manifests = new ConcurrentHashMap<>();
        this.metrics = new StageMetrics(stage.getStageId());
    }

    // ─── Entry point ────────────────────────────────────────────────────
    void run() {
        metrics.recordStart();
        int initialDispatchCount = Math.min(stage.getTerminationDecider().initialBatchSize(targets.size()), targets.size());
        if (initialDispatchCount == 0) {
            transitionTo(State.CREATED, State.SUCCEEDED);
            metrics.recordEnd();
            context.completedStages().add(stage.getStageId());
            listener.onResponse(null);
            return;
        }
        inFlight.set(initialDispatchCount);
        transitionTo(State.CREATED, State.RUNNING);
        for (int i = 0; i < initialDispatchCount; i++) {
            ShardTarget target = pendingTargets.poll();
            if (target == null) break;  // re-entrant completion already drained the queue
            dispatchShardTask(target);
        }
    }

    // ─── Dispatch primitive ────────────────────────────────────────────
    private void dispatchShardTask(ShardTarget target) {
        FragmentExecutionRequest request = new FragmentExecutionRequest(
            context.queryId(), stage.getStageId(), target.shardId(), planAlternatives
        );
        submitter.submit(request, target.node(), new ActionListener<>() {
            @Override
            public void onResponse(FragmentExecutionResponse response) {
                context.searchExecutor().execute(() -> handleResponse(response, target));
            }

            @Override
            public void onFailure(Exception e) {
                handleFailure(e);
            }
        });
    }

    void handleResponse(FragmentExecutionResponse response, ShardTarget target) {
        if (isTerminated()) {
            inFlight.decrementAndGet();
            return;
        }
        if (stage.isShuffleWrite()) {
            manifests.put(target.shardId(), parseManifest(response.getMetadata()));
        } else {
            context.rootSink().feed(response);
        }
        metrics.incrementTasksCompleted();
        onTaskCompletion();
    }

    void handleFailure(Exception e) {
        if (isTerminated()) {
            inFlight.decrementAndGet();
            return;
        }
        failure.compareAndSet(null, e);
        metrics.incrementTasksFailed();
        onTaskCompletion();
    }

    private void onTaskCompletion() {
        int completed = completedTasks.incrementAndGet();

        // Check termination — if decider says stop, finish immediately
        if (state.get() == State.RUNNING) {
            if (stage.getTerminationDecider().shouldTerminate(context.rootSink(), completed, targets.size())) {
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
        metrics.recordEnd();
        Exception captured = failure.get();
        if (captured != null) {
            transitionToTerminal(State.FAILED);
            if (context.parentTask() instanceof CancellableTask ct && ct.isCancelled()) {
                listener.onFailure(new TaskCancelledException("query cancelled"));
            } else {
                listener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", captured));
            }
        } else {
            transitionToTerminal(State.SUCCEEDED);
            if (stage.isShuffleWrite()) {
                context.shuffleManifests().put(stage.getStageId(), manifests);
            }
            context.completedStages().add(stage.getStageId());
            listener.onResponse(null);
        }
    }

    // ─── State machine helpers ──────────────────────────────────────────
    private void transitionTo(State expected, State next) {
        boolean ok = state.compareAndSet(expected, next);
        assert ok : "illegal state transition: expected " + expected + ", was " + state.get();
    }

    private void transitionToTerminal(State terminal) {
        if (!state.compareAndSet(State.TERMINATED, terminal)) {
            state.compareAndSet(State.RUNNING, terminal);
        }
    }

    private boolean isTerminated() {
        State s = state.get();
        return s == State.TERMINATED || s == State.SUCCEEDED || s == State.FAILED;
    }

    private Map<Integer, String> parseManifest(Map<String, String> metadata) {
        Map<Integer, String> manifest = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            manifest.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return manifest;
    }

    // ─── Test accessors (package-private) ───────────────────────────────
    State getState() {
        return state.get();
    }

    StageMetrics getMetrics() {
        return metrics;
    }

    int getCompletedTasks() {
        return completedTasks.get();
    }

    int getInFlight() {
        return inFlight.get();
    }
}
