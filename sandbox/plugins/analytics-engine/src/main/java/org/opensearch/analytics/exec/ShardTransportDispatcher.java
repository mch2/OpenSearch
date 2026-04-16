/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;

/**
 * Transport dispatch component extracted from {@link Scheduler}.
 * Owns {@link TransportService} (or {@link StreamTransportService}), connection lookup,
 * {@code maxConcurrentShardRequests}, and the {@link PendingExecutions}
 * inner class for per-node concurrency gating.
 *
 * <p>Also registers the server-side shard request handler at construction time
 * (delegating fragment execution to {@link AnalyticsSearchService}). Both the
 * outbound dispatch path and the inbound handler live here because they share
 * the same {@link TransportService} reference.
 *
 * <p>When a {@link StreamTransportService} is provided, streaming responses are
 * handled via {@code handleStreamResponse}. When a regular {@link TransportService}
 * is used (e.g., in tests without Arrow Flight), the non-streaming
 * {@code handleResponse} fallback path is taken.
 *
 * <p>The {@link #dispatch} method accepts per-query parameters ({@code parentTask},
 * {@code pendingPerNode}) so that {@link Scheduler} can create a per-query
 * {@link ShardRequestClient} by binding these parameters.
 *
 * @opensearch.internal
 */
public class ShardTransportDispatcher {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final int maxConcurrentShardRequests;

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    /**
     * Guice-injected constructor. Selects {@link StreamTransportService} when
     * available (Arrow Flight configured), otherwise falls back to regular
     * {@link TransportService}. Registers the server-side shard request handler
     * on the selected transport service.
     */
    @Inject
    public ShardTransportDispatcher(
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        this.transportService = streamTransportService != null ? streamTransportService : transportService;
        this.clusterService = clusterService;
        this.maxConcurrentShardRequests = DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS;
        registerShardHandler(this.transportService, searchService, indicesService);
    }

    /**
     * Test-only constructor with explicit max concurrent requests. Skips handler
     * registration since tests either install their own mock handlers or don't
     * exercise the inbound path.
     */
    ShardTransportDispatcher(TransportService transportService, ClusterService clusterService, int maxConcurrentShardRequests) {
        this.transportService = Objects.requireNonNull(transportService, "TransportService must not be null");
        this.clusterService = clusterService;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }

    /**
     * Registers the server-side handler for {@link AnalyticsShardAction#NAME}.
     * Resolves the target shard via {@link IndicesService}, delegates to
     * {@link AnalyticsSearchService#executeFragment}, and writes the response
     * to the channel. Mirrors {@code StreamSearchTransportService.registerStreamRequestHandler}
     * from the search path.
     */
    private static void registerShardHandler(
        TransportService transportService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        transportService.registerRequestHandler(
            AnalyticsShardAction.NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            FragmentExecutionRequest::new,
            (request, channel, task) -> {
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                FragmentExecutionResponse response = searchService.executeFragment(request, shard);
                channel.sendResponse(response);
            }
        );
    }

    /**
     * Resolves a connection to the given node.
     * Mirrors {@code AbstractSearchAsyncAction.getConnection(alias, nodeId)}.
     * For MVP, clusterAlias is ignored (local cluster only).
     *
     * @param clusterAlias the cluster alias (null for local cluster; reserved for future CCS)
     * @param nodeId       the node ID to connect to
     * @return the resolved connection
     */
    Transport.Connection getConnection(String clusterAlias, String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        return transportService.getConnection(node);
    }

    /**
     * Dispatches a fragment execution request to the target data node with
     * per-node concurrency gating. If permits are available for the target
     * node, the request dispatches immediately. Otherwise it is queued and
     * dispatched when a permit is freed.
     *
     * <p>Uses {@link TransportService#sendChildRequest} to propagate the
     * parent task ID to data nodes (enabling task cancellation cascading).
     * Analytics queries always have a parent task.
     *
     * @param request        the fragment execution request
     * @param targetNode     the node hosting the target shard
     * @param listener       the streaming response listener to notify on each batch or failure
     * @param parentTask     the parent task for child-request propagation
     * @param pendingPerNode per-query map of per-node concurrency queues
     */
    public void dispatch(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener listener,
        Task parentTask,
        Map<String, PendingExecutions> pendingPerNode
    ) {
        PendingExecutions pending = pendingPerNode.computeIfAbsent(
            targetNode.getId(),
            n -> new PendingExecutions(maxConcurrentShardRequests)
        );

        TransportResponseHandler<FragmentExecutionResponse> handler = new TransportResponseHandler<>() {
            @Override
            public FragmentExecutionResponse read(StreamInput in) throws IOException {
                return new FragmentExecutionResponse(in);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<FragmentExecutionResponse> stream) {
                try {
                    // Look-ahead pattern from StreamSearchTransportService:
                    // buffer one response ahead to detect isLast
                    FragmentExecutionResponse current;
                    FragmentExecutionResponse last = null;
                    while ((current = stream.nextResponse()) != null) {
                        if (last != null) {
                            listener.onStreamResponse(last, false);
                        }
                        last = current;
                    }
                    if (last != null) {
                        listener.onStreamResponse(last, true);
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    try {
                        stream.close();
                    } catch (Exception ignore) {}
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleResponse(FragmentExecutionResponse response) {
                // Non-streaming fallback (MockTransportService in tests)
                try {
                    listener.onStreamResponse(response, true);
                } finally {
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleException(TransportException e) {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }
        };

        pending.tryRun(() -> {
            try {
                Transport.Connection connection = getConnection(null, targetNode.getId());
                transportService.sendChildRequest(
                    connection,
                    AnalyticsShardAction.NAME,
                    request,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    handler
                );
            } catch (Exception e) {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
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
