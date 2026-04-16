/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;

/**
 * Callback that {@link StageExecutor} uses to recurse on child stages.
 * Avoids a compile-time cycle between {@link StageExecutor} and the
 * recursion driver (currently {@code PlanWalker}).
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ChildDispatcher {

    /**
     * Dispatch a child stage with the given output sink and shard client.
     *
     * @param stage    the child stage to dispatch
     * @param sink     the output sink for this child's results
     * @param client   outbound shard client for transport dispatch
     * @param listener completion listener for this child stage
     */
    void dispatch(Stage stage, ExchangeSink sink, ShardRequestClient client, ActionListener<Void> listener);
}
