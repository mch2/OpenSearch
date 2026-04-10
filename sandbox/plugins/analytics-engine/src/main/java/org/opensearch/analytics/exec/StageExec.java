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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
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
final class StageExec {

    // ─── State machine ──────────────────────────────────────────────────
    enum State { CREATED, RUNNING, TERMINATED, SUCCEEDED, FAILED }

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    // ─── Sliding-window state ───────────────────────────────────────────
    private final AtomicInteger nextTargetIndex = new AtomicInteger(0);
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    // ─── Stage-scoped inputs (immutable after construction) ─────────────
    private final Stage stage;
    private final List<TargetShard> targets;
    private final int totalTargets;
    private final List<FragmentExecutionRequest.PlanAlternative> planAlternatives;
    private final TerminationDecider decider;
    private final boolean collectMetadata;
    private final Map<ShardId, Map<Integer, String>> manifests;
    private final StageMetrics metrics;

    // ─── Cross-stage / per-query references ─────────────────────────────
    private final String queryId;
    private final Executor searchExecutor;
    private final ExchangeSink rootSink;
    private final Set<Integer> completedStages;
    private final Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests;
    private final TaskSubmitter submitter;
    private final ActionListener<Void> listener;

    // ─── Construction ───────────────────────────────────────────────────
    StageExec(
        Stage stage,
        List<TargetShard> targets,
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives,
        boolean collectMetadata,
        Map<ShardId, Map<Integer, String>> manifests,
        StageMetrics metrics,
        String queryId,
        Executor searchExecutor,
        ExchangeSink rootSink,
        Set<Integer> completedStages,
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests,
        TaskSubmitter submitter,
        ActionListener<Void> listener
    ) {
        this.stage = stage;
        this.targets = targets;
        this.totalTargets = targets.size();
        this.planAlternatives = planAlternatives;
        this.decider = stage.getTerminationDecider();
        this.collectMetadata = collectMetadata;
        this.manifests = manifests;
        this.metrics = metrics;
        this.queryId = queryId;
        this.searchExecutor = searchExecutor;
        this.rootSink = rootSink;
        this.completedStages = completedStages;
        this.shuffleManifests = shuffleManifests;
        this.submitter = submitter;
        this.listener = listener;
    }

    // ─── Entry point ────────────────────────────────────────────────────
    void run() {
        int actualBatch = Math.min(decider.initialBatchSize(totalTargets), totalTargets);
        if (actualBatch == 0) {
            transitionTo(State.CREATED, State.SUCCEEDED);
            metrics.recordEnd();
            completedStages.add(stage.getStageId());
            listener.onResponse(null);
            return;
        }
        nextTargetIndex.set(actualBatch);
        inFlight.set(actualBatch);
        transitionTo(State.CREATED, State.RUNNING);
        for (int i = 0; i < actualBatch; i++) {
            submitTask(i);
        }
    }

    // ─── Dispatch primitive ────────────────────────────────────────────
    private void submitTask(int index) {
        TargetShard target = targets.get(index);
        FragmentExecutionRequest request = new FragmentExecutionRequest(
            queryId,
            stage.getStageId(),
            target.shardId(),
            planAlternatives
        );
        submitter.submit(request, target.node(), new ActionListener<>() {
            @Override
            public void onResponse(FragmentExecutionResponse response) {
                searchExecutor.execute(() -> handleResponse(response, target));
            }

            @Override
            public void onFailure(Exception e) {
                handleFailure(e);
            }
        });
    }

    // ─── Response handling (package-private for test drive) ─────────────
    void handleResponse(FragmentExecutionResponse response, TargetShard target) {
        if (isTerminated()) {
            inFlight.decrementAndGet();
            return;
        }
        if (collectMetadata) {
            manifests.put(target.shardId(), parseManifest(response.getMetadata()));
        } else {
            rootSink.feed(response);
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

    // ─── Completion plumbing ────────────────────────────────────────────
    private void onTaskCompletion() {
        int completed = completedTasks.incrementAndGet();

        // Check termination — if decider says stop, finish immediately
        if (state.get() == State.RUNNING) {
            if (decider.shouldTerminate(rootSink, completed, totalTargets)) {
                state.compareAndSet(State.RUNNING, State.TERMINATED);
                finishStageInternal();
                return;
            }
        }

        // Dispatch next target if still running and more remain
        if (state.get() == State.RUNNING) {
            int next = nextTargetIndex.getAndIncrement();
            if (next < totalTargets) {
                inFlight.incrementAndGet();
                submitTask(next);
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
            listener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", captured));
        } else {
            transitionToTerminal(State.SUCCEEDED);
            if (collectMetadata) {
                shuffleManifests.put(stage.getStageId(), manifests);
            }
            completedStages.add(stage.getStageId());
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

    int getCompletedTasks() {
        return completedTasks.get();
    }

    int getInFlight() {
        return inFlight.get();
    }
}
