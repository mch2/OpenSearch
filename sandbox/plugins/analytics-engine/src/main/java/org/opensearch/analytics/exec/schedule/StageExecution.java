/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.schedule;

import org.opensearch.common.Nullable;

/**
 * External view of a stage execution — lifecycle observation + cancellation only.
 * All scheduler/cascade machinery (task dispatch, retry, metadata channel, cascade
 * wiring) lives on {@link AbstractStageExecution} with package-private or protected
 * visibility so it can't be invoked from outside the schedule package.
 *
 * @opensearch.internal
 */
public interface StageExecution {

    int getStageId();

    State getState();

    StageMetrics getMetrics();

    /** Non-null only when state is {@link State#FAILED}. */
    @Nullable
    Exception getFailure();

    /** Idempotent. Transitions to CANCELLED and tears down stage-owned resources. */
    void cancel(String reason);

    /** Append-only state observer. Fired synchronously on every transition. */
    void addStateListener(StageStateListener listener);

    /** Lifecycle states a stage execution moves through. */
    enum State {
        CREATED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELLED;

        public boolean isTerminal() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }
    }
}
