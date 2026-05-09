/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Leaf stage execution that dispatches fragment work to data-node shards.
 *
 * <p>Handles both Arrow streaming and row (codec-decoded) responses, feeding
 * resulting batches into the parent stage's {@link ExchangeSink}.
 *
 * <p>One-shot: constructed, {@link #start()} called once, listener
 * signaled on completion, then discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private static final Logger logger = LogManager.getLogger(ShardFragmentStageExecution.class);

    private final AtomicInteger inFlight = new AtomicInteger(0);

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final ResponseCodec<FragmentExecutionResponse> responseCodec;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher,
        ResponseCodec<FragmentExecutionResponse> responseCodec
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
        this.responseCodec = responseCodec;
    }

    private boolean useArrowStreaming() {
        return dispatcher.isStreamingEnabled();
    }

    @Override
    public void start() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        if (resolved.isEmpty()) {
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(resolved.size());
        for (ExecutionTarget target : resolved) {
            dispatchShardTask((ShardExecutionTarget) target);
        }
    }

    private void dispatchShardTask(ShardExecutionTarget target) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        if (useArrowStreaming()) {
            dispatcher.dispatchFragmentStreaming(
                request,
                target.node(),
                responseListener(FragmentExecutionArrowResponse::getRoot),
                config.parentTask(),
                pending
            );
        } else {
            dispatcher.dispatchFragment(
                request,
                target.node(),
                responseListener(r -> responseCodec.decode(r, config.bufferAllocator())),
                config.parentTask(),
                pending
            );
        }
    }

    private <T extends ActionResponse> StreamingResponseListener<T> responseListener(Function<T, VectorSchemaRoot> toVsr) {
        return new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(T response, boolean isLast) {
                config.searchExecutor().execute(() -> {
                    if (isDone()) {
                        logger.debug(
                            "[stage={}] onStreamResponse short-circuited (state={}); isLast={} response={}",
                            stage.getStageId(),
                            getState(),
                            isLast,
                            response.getClass().getSimpleName()
                        );
                        releaseResponseResources(response);
                        return;
                    }

                    VectorSchemaRoot vsr = toVsr.apply(response);
                    int rows = vsr.getRowCount();
                    logger.debug(
                        "[stage={}] onStreamResponse: feeding batch rows={} isLast={} inFlight={} sinkClass={}",
                        stage.getStageId(),
                        rows,
                        isLast,
                        inFlight.get(),
                        outputSink.getClass().getSimpleName()
                    );
                    try {
                        outputSink.feed(vsr);
                    } catch (Exception e) {
                        // Without this guard, an outputSink.feed(...) exception surfaces only on
                        // the search-executor thread — the stage stays RUNNING, inFlight never
                        // decrements, parentExec.start() never fires, and the query hangs to
                        // QUERY_TIMEOUT instead of propagating the real cause. Exception covers
                        // every FFM/FFI failure mode the native sink can produce
                        // (IllegalStateException on closed arena/handle, WrongThreadException on
                        // cross-thread access, IndexOutOfBoundsException on memory bounds, plus
                        // any RuntimeException the sink raises for stream-merge errors). Errors
                        // (OOM, StackOverflow, AssertionError) propagate to the JVM uncaught
                        // handler — those indicate JVM-level state we can't safely recover from
                        // by re-driving the stage's failure path.
                        vsr.close();
                        captureFailure(new RuntimeException("Stage " + stage.getStageId() + " sink feed failed", e));
                        metrics.incrementTasksFailed();
                        onShardTerminated();
                        return;
                    }
                    metrics.addRowsProcessed(rows);
                    if (isLast) {
                        metrics.incrementTasksCompleted();
                        onShardTerminated();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        };
    }

    private static <T> void releaseResponseResources(T response) {
        if (response instanceof ArrowBatchResponse arrowResp && arrowResp.getRoot() != null) {
            arrowResp.getRoot().close();
        }
    }

    private void onShardTerminated() {
        int after = inFlight.decrementAndGet();
        logger.debug("[stage={}] shard terminated; inFlight={}", stage.getStageId(), after);
        if (after == 0) {
            Exception captured = getFailure();
            try {
                outputSink.close();
            } catch (Exception closeError) {
                if (captured == null) {
                    captureFailure(closeError);
                    captured = closeError;
                }
            }
            transitionTo(captured != null ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED);
        }
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
        // Cancelling the parent task propagates to data-node shard tasks via TaskCancellationService.
        org.opensearch.tasks.Task parentTask = config.parentTask();
        if (parentTask instanceof org.opensearch.tasks.CancellableTask ct && ct.isCancelled() == false) {
            ct.cancel(reason);
        }
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    private boolean isDone() {
        StageExecution.State s = getState();
        return s == StageExecution.State.SUCCEEDED || s == StageExecution.State.FAILED || s == StageExecution.State.CANCELLED;
    }

    private PendingExecutions pendingFor(ShardExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(target.node().getId(), n -> new PendingExecutions(config.maxConcurrentShardRequests()));
    }
}
