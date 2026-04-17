/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Per-stage execution for row-producing DATA_NODE stages (scans, filters,
 * partial aggregates). Dispatches shard requests via
 * {@link AnalyticsSearchTransportService#dispatchFragment}, collects streaming
 * {@link FragmentExecutionResponse} batches, and feeds them into the stage's
 * {@link org.opensearch.analytics.backend.ExchangeSink}.
 *
 * <p>Replaces the scan path that previously lived in the generic
 * fan-out execution + sink-feeding handler.
 *
 * <p>Lifecycle: {@code CREATED → RUNNING → SUCCEEDED | FAILED | CANCELLED}.
 * Instances are one-shot: constructed, {@link #start()} called once,
 * listener signaled once, discarded.
 *
 * <p>No {@code completedStages} tracking — that responsibility moves to
 * the caller (PlanWalker / scheduler) in a later change.
 *
 * @opensearch.internal
 */
public final class ShardScanStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    // Immutable config
    private final QueryContext config;
    private final ExchangeSink sink;
    private final List<ShardTarget> targets;
    private final Function<ShardTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    public ShardScanStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink sink,
        List<ShardTarget> targets,
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage);
        this.config = config;
        this.sink = sink;
        this.targets = targets;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() {
        if (targets.isEmpty()) {
            // CREATED → SUCCEEDED directly. transitionTo stamps both start and end.
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        // TODO: Introduce Shard Filter & Termination Decider logic to this execution type
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(targets.size());
        for (ShardTarget target : targets) {
            dispatchShardTask(target);
        }
    }

    private void dispatchShardTask(ShardTarget target) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);

        // With PR 21253's ArrowBatchResponse, the standard read(StreamInput) path
        // produces a FragmentExecutionResponse with the Arrow root directly —
        // no ArrowStreamHandler or readArrow needed.
        var listener = new StreamingResponseListener<FragmentExecutionResponse>() {
            @Override
            public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
                if (isDone()) return;

                // response is null on the isLast=true completion signal
                // (Flight root is reused, so data batches are sent with
                // isLast=false and completion is signaled separately).
                if (response != null) {
                    VectorSchemaRoot flightRoot = response.getArrowRoot();
                    // Zero-copy transfer from Flight's reused root into an
                    // independent root. onStreamResponse runs synchronously
                    // in the handleStreamResponse loop — the next nextResponse()
                    // (which overwrites Flight's root) hasn't been called yet.
                    // transfer() moves buffer pointers (O(1)), leaving
                    // Flight's root empty for the next batch.
                    BufferAllocator flightAlloc = flightRoot.getFieldVectors().get(0).getAllocator();
                    BufferAllocator batchAlloc = flightAlloc.newChildAllocator("batch", 0, Long.MAX_VALUE);
                    VectorSchemaRoot transferred = VectorSchemaRoot.create(flightRoot.getSchema(), batchAlloc);
                    for (int i = 0; i < flightRoot.getFieldVectors().size(); i++) {
                        flightRoot.getFieldVectors().get(i)
                            .makeTransferPair(transferred.getFieldVectors().get(i))
                            .transfer();
                    }
                    transferred.setRowCount(flightRoot.getRowCount());
                    sink.feed(transferred);
                    metrics.addRowsProcessed(transferred.getRowCount());
                }

                if (isLast) {
                    metrics.incrementTasksCompleted();
                    onTaskCompletion();
                }
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onTaskCompletion();
            }
        };

        dispatcher.dispatchFragment(request, target.node(), listener, config.parentTask(), pending);
    }

    private void onTaskCompletion() {
        completedTasks.incrementAndGet();
        if (inFlight.decrementAndGet() == 0) {
            finishStageInternal();
        }
    }

    private void finishStageInternal() {
        Exception captured = getFailure();
        StageExecution.State target = (captured != null) ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED;
        transitionTo(target);
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
    }

    @Override
    public ExchangeSink sink() {
        return sink;
    }

    /** Returns the sink this execution writes batches into. */
    public ExchangeSink getSink() {
        return sink;
    }

    private boolean isDone() {
        StageExecution.State s = getState();
        return s == StageExecution.State.SUCCEEDED || s == StageExecution.State.FAILED || s == StageExecution.State.CANCELLED;
    }

    private PendingExecutions pendingFor(ShardTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequests())
        );
    }
}
