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
import org.opensearch.analytics.backend.ExchangeSource;
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
 * {@link AnalyticsSearchTransportService#dispatchFragment}, receives streaming
 * native-Arrow responses, and feeds the resulting
 * {@link org.apache.arrow.vector.VectorSchemaRoot} batches into the stage's
 * output {@link ExchangeSink}.
 *
 * <p>Client-side buffers arrive in Flight's reused client root; this stage
 * transfers them (zero-copy pointer move) into an independent VSR owned by a
 * per-batch child allocator before feeding the sink, so the next batch's
 * arrival does not overwrite data the sink has captured.
 *
 * <p>Implements {@link DataProducer} because it writes batches into a sink
 * owned by its parent stage. Does not implement {@link DataConsumer} because
 * it is a leaf stage with no children.
 *
 * <p>Lifecycle: {@code CREATED → RUNNING → SUCCEEDED | FAILED | CANCELLED}.
 * Instances are one-shot: constructed, {@link #start()} called once,
 * listener signaled once, discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final AtomicInteger inFlight = new AtomicInteger(0);

    // Immutable config
    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final List<ShardTarget> targets;
    private final Function<ShardTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        List<ShardTarget> targets,
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
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
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(targets.size());
        for (ShardTarget target : targets) {
            dispatchShardTask(target);
        }
    }

    private void dispatchShardTask(ShardTarget target) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        dispatcher.dispatchFragment(request, target.node(), new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
                if (isDone()) return;

                // response is null only on the completion signal — the transport
                // handler fires (null, true) once after the last data batch so the
                // stage knows the shard is done. Data batches always arrive with
                // isLast=false.
                if (response != null) {
                    // Flight reuses its client-side root across batches, so we transfer
                    // the buffers out into an independent VSR before the next
                    // nextResponse() overwrites them. Target lives on a per-batch child
                    // allocator of flightAlloc so the transfer is intra-tree
                    // (direct-target transfer into flightAlloc breaks Arrow's
                    // buffer-association check on the downstream C-data export path).
                    //
                    // batchAlloc is NOT closed synchronously: when the sink is a
                    // Datafusion input, feed() → pushBatch exports via Arrow C-data,
                    // which increments native ref counts that Rust releases
                    // asynchronously. A sync close would see those still-held refs as
                    // leaked. batchAlloc remains a child of flightAlloc and is cleaned
                    // up when flightAlloc tears down, by which time Rust has released.
                    VectorSchemaRoot flightRoot = response.getArrowRoot();
                    BufferAllocator flightAlloc = flightRoot.getFieldVectors().get(0).getAllocator();
                    BufferAllocator batchAlloc = flightAlloc.newChildAllocator("batch", 0, Long.MAX_VALUE);
                    VectorSchemaRoot transferred = VectorSchemaRoot.create(flightRoot.getSchema(), batchAlloc);
                    for (int i = 0; i < flightRoot.getFieldVectors().size(); i++) {
                        flightRoot.getFieldVectors().get(i).makeTransferPair(transferred.getFieldVectors().get(i)).transfer();
                    }
                    transferred.setRowCount(flightRoot.getRowCount());

                    int rowCount = transferred.getRowCount();
                    outputSink.feed(transferred);
                    metrics.addRowsProcessed(rowCount);
                }

                if (isLast) {
                    metrics.incrementTasksCompleted();
                    onShardTerminated();
                }
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        }, config.parentTask(), pending);
    }

    private void onShardTerminated() {
        if (inFlight.decrementAndGet() == 0) {
            Exception captured = getFailure();
            transitionTo(captured != null ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED);
        }
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
    }

    @Override
    public ExchangeSink outputSink() {
        return outputSink;
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

    private PendingExecutions pendingFor(ShardTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequests())
        );
    }
}
