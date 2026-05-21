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
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;

/**
 * Stateless transport dispatch component for fragment requests. Owns the
 * {@link StreamTransportService} (analytics-engine is streaming-only) and
 * connection lookup.
 *
 * <p>Does NOT track per-query or per-node concurrency state — callers provide
 * their own {@link PendingExecutions} instance to gate dispatch concurrency.
 *
 * @opensearch.internal
 */
@Singleton
public class AnalyticsSearchTransportService {
    private static final Logger logger = LogManager.getLogger(AnalyticsSearchTransportService.class);

    private final StreamTransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public AnalyticsSearchTransportService(
        StreamTransportService streamTransportService,
        ClusterService clusterService,
        AnalyticsSearchService searchService,
        IndicesService indicesService,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        if (streamTransportService == null) {
            throw new IllegalStateException(
                "analytics-engine requires the STREAM_TRANSPORT feature flag to be enabled "
                    + "("
                    + FeatureFlags.STREAM_TRANSPORT
                    + "=true)"
            );
        }
        searchService.setTaskResourceTrackingService(taskResourceTrackingService);
        this.transportService = streamTransportService;
        this.clusterService = clusterService;
        registerStreamingFragmentHandler(this.transportService, searchService, indicesService);
    }

    private static void registerStreamingFragmentHandler(
        StreamTransportService transportService,
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
                logger.warn(
                    "[shard-handoff] HANDLER invoked (data-node received request) shard={} thread=[{}]",
                    request.getShardId(),
                    Thread.currentThread().getName()
                );
                IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
                searchService.executeFragmentStreamingAsync(
                    request,
                    shard,
                    (AnalyticsShardTask) task,
                    new AnalyticsSearchService.StreamingFragmentResponseHandler() {
                        @Override
                        public void onBatch(EngineResultBatch batch) throws Exception {
                            channel.sendResponseBatch(new FragmentExecutionArrowResponse(batch.getArrowRoot()));
                        }

                        @Override
                        public void onComplete() {
                            channel.completeStream();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof StreamException se && se.getErrorCode() == StreamErrorCode.CANCELLED) {
                                return;
                            }
                            try {
                                channel.sendResponse(e);
                            } catch (Exception ignored) {}
                        }
                    },
                    transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
                );
            }
        );
    }

    Transport.Connection getConnection(String clusterAlias, String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        return transportService.getConnection(node);
    }

    public void dispatchFragmentStreaming(
        FragmentExecutionRequest request,
        DiscoveryNode targetNode,
        StreamingResponseListener<FragmentExecutionArrowResponse> listener,
        Task parentTask,
        PendingExecutions pending
    ) {
        TransportResponseHandler<FragmentExecutionArrowResponse> handler = new TransportResponseHandler<>() {
            @Override
            public FragmentExecutionArrowResponse read(StreamInput in) throws IOException {
                return new FragmentExecutionArrowResponse(in);
            }

            @Override
            public boolean skipsDeserialization() {
                return true;
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleStreamResponse(StreamTransportResponse<FragmentExecutionArrowResponse> stream) {
                logger.warn(
                    "[shard-handoff] consumer handleStreamResponse START node={} thread=[{}]",
                    targetNode.getId(),
                    Thread.currentThread().getName()
                );
                long frames = 0;
                try {
                    FragmentExecutionArrowResponse current;
                    FragmentExecutionArrowResponse last = null;
                    while ((current = stream.nextResponse()) != null) {
                        frames++;
                        if (last != null) {
                            listener.onStreamResponse(last, false);
                        }
                        last = current;
                    }
                    if (last != null) {
                        logger.warn("[shard-handoff] consumer stream ended frames={} — firing isLast=true", frames);
                        listener.onStreamResponse(last, true);
                    } else {
                        logger.warn("[shard-handoff] consumer stream ended with ZERO frames (last==null) — isLast NEVER fired!", frames);
                    }
                } catch (Exception e) {
                    logger.warn("[shard-handoff] consumer handleStreamResponse threw after frames={} err={}", frames, e.toString());
                    listener.onFailure(e);
                } finally {
                    try {
                        stream.close();
                    } catch (Exception ignore) {}
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleResponse(FragmentExecutionArrowResponse response) {
                logger.warn("[shard-handoff] consumer handleResponse (non-stream) node={} — firing isLast=true", targetNode.getId());
                try {
                    listener.onStreamResponse(response, true);
                } finally {
                    pending.finishAndRunNext();
                }
            }

            @Override
            public void handleException(TransportException e) {
                logger.warn("[shard-handoff] consumer handleException node={} err={}", targetNode.getId(), e.toString());
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }
        };

        TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build();
        logger.warn(
            "[shard-handoff] dispatchFragmentStreaming calling tryRun node={} thread=[{}]",
            targetNode.getId(),
            Thread.currentThread().getName()
        );
        pending.tryRun(() -> {
            try {
                logger.warn(
                    "[shard-handoff] tryRun BODY running (permit acquired) node={} thread=[{}] — sending request",
                    targetNode.getId(),
                    Thread.currentThread().getName()
                );
                Transport.Connection connection = getConnection(null, targetNode.getId());
                transportService.sendChildRequest(connection, FragmentExecutionAction.NAME, request, parentTask, options, handler);
                logger.warn("[shard-handoff] sendChildRequest RETURNED node={}", targetNode.getId());
            } catch (Exception e) {
                try {
                    listener.onFailure(e);
                } finally {
                    pending.finishAndRunNext();
                }
            }
        });
    }
}
