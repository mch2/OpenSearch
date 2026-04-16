/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link StageExecution} implementation for LOCAL stages. Owns the
 * {@link LocalStageContext} lifecycle (start, finalize, fail, cancel)
 * and ensures the downstream listener is signaled exactly once.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → (SUCCEEDED | FAILED | CANCELLED)}
 *
 * <p>Instances are one-shot: constructed, {@code start()} called once,
 * listener signaled once, discarded.
 *
 * @opensearch.internal
 */
final class LocalStageExecution implements StageExecution {

    private static final Logger logger = LogManager.getLogger(LocalStageExecution.class);

    private final Stage stage;
    private final LocalStageContext ctx;
    private final ActionListener<Void> listener;
    private final StageMetrics metrics;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);
    private final AtomicBoolean signaled = new AtomicBoolean(false);

    LocalStageExecution(Stage stage, LocalStageContext ctx, ActionListener<Void> listener, StageMetrics metrics) {
        this.stage = stage;
        this.ctx = ctx;
        this.listener = listener;
        this.metrics = metrics;
        logger.info("[LocalStage] CREATED stageId={} childCount={}", stage.getStageId(), stage.getChildStages().size());
    }

    void start() {
        logger.info("[LocalStage] start() stageId={}", stage.getStageId());
        metrics.recordStart();
        state.compareAndSet(State.CREATED, State.RUNNING);
    }

    /**
     * Called by the walker once all children have completed successfully.
     * Delegates to the backend's {@link LocalStageContext#asyncFinalize}
     * which drains output and signals the listener.
     */
    void finalizeStage() {
        if (state.get() != State.RUNNING) return;
        logger.info("[LocalStage] finalizeStage() ENTRY stageId={}", stage.getStageId());
        ctx.asyncFinalize(ActionListener.wrap(v -> {
            if (transitionTerminal(State.SUCCEEDED) && signaled.compareAndSet(false, true)) {
                metrics.recordEnd();
                logger.info("[LocalStage] listener.onResponse stageId={}", stage.getStageId());
                listener.onResponse(null);
            }
        }, e -> {
            if (transitionTerminal(State.FAILED) && signaled.compareAndSet(false, true)) {
                metrics.recordEnd();
                metrics.incrementTasksFailed();
                logger.info("[LocalStage] listener.onFailure stageId={} cause={}", stage.getStageId(), e.getMessage());
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Called by the walker when any child stage fails before finalize.
     * Closes the backend context and signals the listener with the failure.
     */
    void failChildStage(Exception e) {
        logger.info("[LocalStage] failChildStage stageId={} cause={}", stage.getStageId(), e.getMessage());
        Throwable rootCause = e;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        logger.info("[LocalStage] failChildStage root cause: {}", rootCause.toString());
        if (transitionTerminal(State.FAILED) && signaled.compareAndSet(false, true)) {
            try {
                ctx.close();
            } catch (Exception ignore) {}
            metrics.recordEnd();
            metrics.incrementTasksFailed();
            listener.onFailure(e);
        }
    }

    @Override
    public void cancel(String reason) {
        logger.info("[LocalStage] cancel stageId={} reason={}", stage.getStageId(), reason);
        if (transitionTerminal(State.CANCELLED) && signaled.compareAndSet(false, true)) {
            try {
                ctx.close();
            } catch (Exception ignore) {}
            metrics.recordEnd();
            listener.onFailure(new TaskCancelledException(reason));
        }
    }

    @Override
    public int getStageId() {
        return stage.getStageId();
    }

    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public StageMetrics getMetrics() {
        return metrics;
    }

    private boolean transitionTerminal(State terminal) {
        return state.compareAndSet(State.CREATED, terminal)
            || state.compareAndSet(State.RUNNING, terminal)
            || state.compareAndSet(State.TERMINATED, terminal);
    }
}
