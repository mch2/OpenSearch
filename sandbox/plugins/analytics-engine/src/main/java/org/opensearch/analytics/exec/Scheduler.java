/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinator-side orchestrator. Manages {@link PlanWalker} lifecycle via a
 * walker pool and gates per-node concurrency via {@link PendingExecutions}.
 *
 * <p>Per-node concurrency gating is per-query: each query gets its own
 * {@code PendingExecutions} map so concurrent queries independently throttle
 * at {@code maxConcurrentShardRequests} per node — matching the model in
 * {@code AbstractSearchAsyncAction}.
 *
 * <p>Uses {@link TransportService#sendRequest} to dispatch tasks to the target
 * data node, which triggers {@link TransportAnalyticsShardAction} on the remote
 * node. For local nodes, the transport layer short-circuits to a direct call.
 *
 * <p>Created by {@link DefaultPlanExecutor} with the Guice-injected
 * {@link TransportService} and {@code maxConcurrentShardRequests}.
 *
 * @opensearch.internal
 */
public class Scheduler {
    private final TransportService transportService;
    private final int maxConcurrentShardRequests;
    private final Map<String, PlanWalker> walkerPool = new ConcurrentHashMap<>();

    public Scheduler(TransportService transportService, int maxConcurrentShardRequests) {
        this.transportService = transportService;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }

    /**
     * Executes a PlanWalker asynchronously. Caches the walker during execution
     * and removes it on completion (success or failure). Extracts the
     * {@link AnalyticsQueryTask} from the walker for parent-child task propagation.
     *
     * <p>Creates a per-query {@code pendingExecutionsPerNode} map so that
     * each query independently gates at {@code maxConcurrentShardRequests}
     * per node. The map is GC'd when the query completes.
     */
    public void execute(PlanWalker walker, ActionListener<Iterable<Object[]>> listener) {
        walkerPool.put(walker.getQueryId(), walker);
        Task parentTask = walker.getParentTask();
        Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();
        walker.walk((req, node, l) -> dispatchShardRequest(req, node, l, parentTask, pendingPerNode), ActionListener.wrap(result -> {
            walkerPool.remove(walker.getQueryId());
            listener.onResponse(result);
        }, e -> {
            walkerPool.remove(walker.getQueryId());
            listener.onFailure(e);
        }));
    }

    /**
     * Dispatches a task to the target data node with per-node concurrency gating.
     * If permits are available for the target node, the task dispatches immediately.
     * Otherwise it is queued and dispatched when a permit is freed.
     *
     * <p>When {@code parentTask} is non-null, uses
     * {@link TransportService#sendChildRequest} to propagate the parent task ID
     * to data nodes (enabling task cancellation cascading). When null (test mode),
     * falls back to {@link TransportService#sendRequest}.
     */
    private void dispatchShardRequest(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        ActionListener<FragmentExecutionResponse> listener,
        Task parentTask,
        Map<String, PendingExecutions> pendingPerNode
    ) {
        PendingExecutions pending = pendingPerNode.computeIfAbsent(
            targetNode.getId(),
            n -> new PendingExecutions(maxConcurrentShardRequests)
        );

        ActionListenerResponseHandler<FragmentExecutionResponse> handler = new ActionListenerResponseHandler<>(
            ActionListener.wrap(response -> {
                try {
                    listener.onResponse(response);
                } finally {
                    pending.finishAndRunNext();
                }
            }, e -> {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }),
            FragmentExecutionResponse::new
        );

        pending.tryRun(() -> {
            if (parentTask != null) {
                transportService.sendChildRequest(
                    targetNode,
                    AnalyticsShardAction.NAME,
                    request,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    handler
                );
            } else {
                transportService.sendRequest(targetNode, AnalyticsShardAction.NAME, request, handler);
            }
        });
    }

    /**
     * Permit-based concurrency queue per node. Same pattern as
     * {@code AbstractSearchAsyncAction.PendingExecutions} in OpenSearch core.
     *
     * <p>When permits are available, tasks run immediately. When all permits are
     * taken, tasks queue in a FIFO {@link ArrayDeque}. Completing a task releases
     * a permit and dequeues+runs the next waiting task.
     */
    static final class PendingExecutions {
        private final int permits;
        private int permitsTaken = 0;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "permits must be > 0: " + permits;
            this.permits = permits;
        }

        void tryRun(Runnable runnable) {
            Runnable toExecute = tryQueue(runnable);
            if (toExecute != null) {
                toExecute.run();
            }
        }

        void finishAndRunNext() {
            synchronized (this) {
                permitsTaken--;
                assert permitsTaken >= 0 : "illegal permits: " + permitsTaken;
            }
            tryRun(null);
        }

        private synchronized Runnable tryQueue(Runnable runnable) {
            Runnable toExecute = null;
            if (permitsTaken < permits) {
                permitsTaken++;
                toExecute = runnable;
                if (toExecute == null) {
                    toExecute = queue.poll();
                }
                if (toExecute == null) {
                    permitsTaken--;
                }
            } else if (runnable != null) {
                queue.add(runnable);
            }
            return toExecute;
        }
    }
}
