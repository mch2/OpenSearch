/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Stateless transport dispatch component for scan requests. Owns
 * {@link TransportService} (or {@link StreamTransportService}) and
 * connection lookup. Does NOT track per-query or per-node concurrency
 * state — callers provide their own {@link PendingExecutions} instance
 * to gate dispatch concurrency.
 *
 * <p>Also registers the server-side scan request handler at construction
 * time (delegating fragment execution to {@link AnalyticsSearchService}).
 *
 * <p>Marked {@link Singleton} because the constructor has a side effect —
 * registering the transport request handler — and double-registration throws.
 *
 * @opensearch.internal
 */
@Singleton
public class AnalyticsSearchTransportService {
    private final TransportService transportService;
    private final ClusterService clusterService;

    /**
     * Guice-injected constructor. Selects {@link StreamTransportService} when
     * available (Arrow Flight configured), otherwise falls back to regular
     * {@link TransportService}. Registers the server-side scan request handler.
     */
    @Inject
    public AnalyticsSearchTransportService(
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        this.transportService = streamTransportService != null ? streamTransportService : transportService;
        this.clusterService = clusterService;
        LogManager.getLogger(AnalyticsSearchTransportService.class)
            .info("[AnalyticsSearchTransportService] using transport: {} (stream={}, flight={})",
                this.transportService.getClass().getSimpleName(),
                streamTransportService != null,
                streamTransportService != null ? streamTransportService.getClass().getSimpleName() : "null");
        registerFragmentHandler(this.transportService, searchService, indicesService);
    }

    /**
     * Test-only constructor. Skips handler registration since tests either
     * install their own mock handlers or don't exercise the inbound path.
     */
    public AnalyticsSearchTransportService(TransportService transportService, ClusterService clusterService) {
        this.transportService = Objects.requireNonNull(transportService, "TransportService must not be null");
        this.clusterService = clusterService;
    }

    /**
     * Registers the server-side handler for {@link FragmentExecutionAction#NAME}.
     * Routes {@link FragmentExecutionRequest} to
     * {@link AnalyticsSearchService#executeFragmentStreaming} which streams
     * batches directly via the transport channel.
     */
    private static void registerFragmentHandler(
        TransportService transportService,
        AnalyticsSearchService searchService,
        IndicesService indicesService
    ) {
        transportService.registerRequestHandler(
            FragmentExecutionAction.NAME,
            ThreadPool.Names.SAME,
            false,
            true,
            AdmissionControlActionType.SEARCH,
            FragmentExecutionRequest::new,
            (request, channel, task) -> {
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                AnalyticsShardTask shardTask = task instanceof AnalyticsShardTask ? (AnalyticsShardTask) task : null;
                searchService.executeFragmentStreaming(request, shard, shardTask, channel);
            }
        );
    }

    /**
     * Resolves the connection to the given target node via this class's
     * {@link ClusterService} and {@link TransportService}.
     */
    Transport.Connection getConnection(String clusterAlias, String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        return transportService.getConnection(node);
    }

    /**
     * Dispatches a scan request to the target data node, gated by the
     * caller-provided {@link PendingExecutions}. Uses the typed
     * {@link FragmentExecutionAction} and delivers streaming {@link FragmentExecutionResponse}
     * batches to the listener.
     *
     * @param request    the fragment execution request
     * @param targetNode the node hosting the target shard
     * @param listener   the streaming response listener for scan batches
     * @param parentTask the parent task for child-request propagation
     * @param pending    the per-node concurrency gate owned by the caller
     */
    public void dispatchFragment(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        TransportResponseHandler<FragmentExecutionResponse> handler = new FragmentResponseHandler(listener, pending);

        pending.tryRun(() -> {
            try {
                Transport.Connection connection = getConnection(null, targetNode.getId());
                transportService.sendChildRequest(
                    connection,
                    FragmentExecutionAction.NAME,
                    request,
                    parentTask,
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
     * Response handler for fragment execution. With PR 21253's
     * {@link ArrowBatchResponse}, the standard {@code read(StreamInput)} path
     * produces a {@link FragmentExecutionResponse} with the Arrow root directly
     * via {@code VectorStreamInput.getRoot()} — no separate ArrowStreamHandler
     * needed.
     */
    private static class FragmentResponseHandler implements StreamTransportResponseHandler<FragmentExecutionResponse> {

        private final StreamingResponseListener<FragmentExecutionResponse> listener;
        private final PendingExecutions pending;

        FragmentResponseHandler(StreamingResponseListener<FragmentExecutionResponse> listener, PendingExecutions pending) {
            this.listener = listener;
            this.pending = pending;
        }

        @Override
        public FragmentExecutionResponse read(StreamInput in) throws IOException {
            LogManager.getLogger(AnalyticsSearchTransportService.class)
                .info("[FragmentResponseHandler] read() called, StreamInput type: {}", in.getClass().getSimpleName());
            return new FragmentExecutionResponse(in);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void handleStreamResponse(StreamTransportResponse<FragmentExecutionResponse> stream) {
            try {
                // Process each batch immediately before the next nextResponse()
                // overwrites Flight's reused root. Every batch gets isLast=false;
                // after the stream ends, fire one more onStreamResponse with
                // isLast=true (null response) to signal completion.
                FragmentExecutionResponse current;
                while ((current = stream.nextResponse()) != null) {
                    listener.onStreamResponse(current, false);
                }
                // Stream exhausted — signal completion
                listener.onStreamResponse(null, true);
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
    }
}
