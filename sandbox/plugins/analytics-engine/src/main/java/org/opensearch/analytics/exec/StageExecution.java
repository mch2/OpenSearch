/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

/**
 * One-shot execution unit for a single stage. Provides state, metrics,
 * and a cancellation hook. Implementations:
 * <ul>
 *   <li>{@link FanOutStageExecution} — fan-out dispatch to data nodes</li>
 *   <li>{@link LocalStageExecution} — coordinator-local backend-provided stage</li>
 * </ul>
 *
 * <p>Tracked in {@code QueryState.stageExecutions} for the duration of
 * execution so that {@code DefaultPlanExecutor} can push cancellation
 * to in-flight stages on failure.
 *
 * @opensearch.internal
 */
public interface StageExecution {

    int getStageId();

    State getState();

    StageMetrics getMetrics();

    /**
     * Idempotent. Transitions state to CANCELLED, tears down
     * stage-owned resources (in-flight transport, sinks, drain threads).
     */
    void cancel(String reason);

    enum State {
        CREATED,
        RUNNING,
        TERMINATED,
        SUCCEEDED,
        FAILED,
        CANCELLED
    }
}
