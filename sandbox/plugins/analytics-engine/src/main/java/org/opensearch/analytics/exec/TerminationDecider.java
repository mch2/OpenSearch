/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

/**
 * Controls sliding-window dispatch for a stage. Determines the initial batch
 * size and whether to stop submitting more tasks after each completion.
 * Called by PlanWalker during dispatchStage() to control task submission.
 *
 * @opensearch.internal
 */
public interface TerminationDecider {

    /**
     * How many tasks to submit in the initial batch.
     *
     * @param totalTargets the total number of resolved target shards
     * @return the number of tasks to submit initially
     */
    int initialBatchSize(int totalTargets);

    /**
     * After each task completes, should we stop submitting more?
     *
     * @param sink           the exchange sink accumulating results
     * @param completedTasks the number of tasks completed so far
     * @param totalTasks     the total number of tasks
     * @return {@code true} to stop submitting, {@code false} to continue
     */
    boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks);

    /** Dispatch all targets at once, never terminate early. */
    TerminationDecider DISPATCH_ALL = new TerminationDecider() {
        @Override
        public int initialBatchSize(int totalTargets) {
            return totalTargets;
        }

        @Override
        public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
            return false;
        }
    };
}
