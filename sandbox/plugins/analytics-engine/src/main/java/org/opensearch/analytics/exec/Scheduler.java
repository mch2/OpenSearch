/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinator-side query orchestrator. Manages the walker pool and binds
 * the per-query {@link ShardRequestClient} from the shared
 * {@link ShardTransportDispatcher}.
 *
 * <p>Walker creation is the caller's responsibility ({@link DefaultPlanExecutor}).
 * Scheduler is pure orchestration — it takes an already-built walker, binds
 * per-query transport state, and kicks off the walk.
 *
 * @opensearch.internal
 */
public class Scheduler {
    private final ShardTransportDispatcher dispatcher;
    private final StageExecutor stageExecutor;
    private final Map<String, PlanWalker> walkerPool = new ConcurrentHashMap<>();

    @Inject
    public Scheduler(ShardTransportDispatcher dispatcher, StageExecutor stageExecutor) {
        this.dispatcher = dispatcher;
        this.stageExecutor = stageExecutor;
    }

    /** Returns the shared stage executor for callers that need to construct a {@link PlanWalker}. */
    public StageExecutor getStageExecutor() {
        return stageExecutor;
    }

    /**
     * Executes a pre-built {@link PlanWalker} asynchronously. Manages the walker
     * pool lifecycle and binds a per-query {@link ShardRequestClient} from the
     * shared {@link ShardTransportDispatcher}.
     *
     * @param walker   the walker to execute (created by the caller with a {@link QueryContext} and {@link QueryState})
     * @param listener completion listener — receives the final result rows
     */
    public void execute(PlanWalker walker, ActionListener<Iterable<Object[]>> listener) {
        String queryId = walker.getQueryId();
        walkerPool.put(queryId, walker);

        // Per-query transport concurrency state — local to this query's execution
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        // Per-query ShardRequestClient: binds dispatcher with parentTask + pendingPerNode
        ShardRequestClient client = (request, node, streamListener) -> dispatcher.dispatch(
            request,
            node,
            streamListener,
            walker.getParentTask(),
            pendingPerNode
        );

        walker.walk(client, ActionListener.wrap(result -> {
            walkerPool.remove(queryId);
            listener.onResponse(result);
        }, e -> {
            walkerPool.remove(queryId);
            listener.onFailure(e);
        }));
    }

    // TODO: Vend metrics from walker pool
}
