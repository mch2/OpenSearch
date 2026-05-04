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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link StageExecution} implementation for COORDINATOR_REDUCE stages. Holds a
 * backend-provided {@link ExchangeSink} (from {@link org.opensearch.analytics.spi.ExchangeSinkProvider})
 * and routes each child stage's output into the correct input of that sink via
 * {@link #inputSink(int)}.
 *
 * <p>Lifecycle:
 * {@code CREATED → RUNNING → (SUCCEEDED | FAILED | CANCELLED)}
 *
 * <p>For multi-input stages (joins), each child gets its own per-child wrapper
 * sink that pins the {@code inputIndex} derived from the child's position in
 * {@link Stage#getChildStages()}. Calls into the wrapper land on
 * {@link ExchangeSink#feed(int, VectorSchemaRoot)} of the shared backend sink.
 * Wrappers' {@code close()} is a no-op — only the parent's {@code start} /
 * {@code failFromChild} / {@code cancel} paths close the backend sink.
 *
 * @opensearch.internal
 */
final class LocalStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(LocalStageExecution.class);

    private final ExchangeSink backendSink;
    private final ExchangeSink downstream;
    private final Map<Integer, Integer> childStageToInputIndex;

    public LocalStageExecution(Stage stage, ExchangeSink backendSink, ExchangeSinkContext ctx, ExchangeSink downstream) {
        super(stage);
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.childStageToInputIndex = buildChildStageIndex(ctx.inputs());
        logger.info(
            "[LocalStage] CREATED stageId={} childCount={} inputCount={}",
            stage.getStageId(),
            stage.getChildStages().size(),
            ctx.inputs().size()
        );
    }

    private static Map<Integer, Integer> buildChildStageIndex(List<ExchangeSinkContext.InputDescriptor> inputs) {
        Map<Integer, Integer> map = new HashMap<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            map.put(inputs.get(i).childStageId(), i);
        }
        return Map.copyOf(map);
    }

    /**
     * Returns a per-child wrapper sink. Routes {@code feed} calls to the shared
     * backend sink with the input index derived from {@code childStageId}.
     * The wrapper's {@code close} is a no-op — backend sink lifecycle is owned
     * by this stage's {@code start}/{@code failFromChild}/{@code cancel}.
     */
    @Override
    public ExchangeSink inputSink(int childStageId) {
        Integer idx = childStageToInputIndex.get(childStageId);
        if (idx == null) {
            throw new IllegalArgumentException(
                "no input descriptor for childStageId=" + childStageId + " (have " + childStageToInputIndex.keySet() + ")"
            );
        }
        int inputIndex = idx;
        return new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {
                backendSink.feed(inputIndex, batch);
            }

            @Override
            public void feed(int inputIndexUnused, VectorSchemaRoot batch) {
                // Wrappers receive single-input feed() from upstream — they pin the
                // index. If a multi-input feed() reaches this wrapper, it's a wiring
                // bug; ignore the passed index and route to the pinned one.
                backendSink.feed(inputIndex, batch);
            }

            @Override
            public void close() {
                // No-op — parent stage owns backendSink lifecycle.
            }
        };
    }

    /**
     * Returns the downstream sink as an {@link ExchangeSource}. The backend sink's
     * {@code close()} drains native batches into this same downstream as the
     * last step of {@link #start()}, so by the time the walker reads via
     * {@code outputSource().readResult()} every result batch is already buffered
     * here.
     */
    @Override
    public ExchangeSource outputSource() {
        if (downstream instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException(
            "downstream sink " + downstream.getClass().getSimpleName() + " does not implement ExchangeSource"
        );
    }

    @Override
    public void start() {
        if (transitionTo(State.RUNNING) == false) return;
        logger.info("[LocalStage] start() stageId={}", stage.getStageId());
        try {
            backendSink.close();
            if (transitionTo(State.SUCCEEDED)) {
                logger.info("[LocalStage] SUCCEEDED stageId={}", stage.getStageId());
            }
        } catch (Exception e) {
            captureFailure(e);
            if (transitionTo(State.FAILED)) {
                metrics.incrementTasksFailed();
                logger.info("[LocalStage] FAILED stageId={} cause={}", stage.getStageId(), e.getMessage());
            }
        }
    }

    @Override
    public boolean failFromChild(Exception cause) {
        logger.error(new ParameterizedMessage("[LocalStage] failFromChild stageId={}", stage.getStageId()), cause);
        captureFailure(cause);
        if (transitionTo(State.FAILED)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
            metrics.incrementTasksFailed();
            return true;
        }
        return false;
    }

    @Override
    public void cancel(String reason) {
        logger.info("[LocalStage] cancel stageId={} reason={}", stage.getStageId(), reason);
        if (transitionTo(State.CANCELLED)) {
            try {
                backendSink.close();
            } catch (Exception ignore) {}
        }
    }
}
