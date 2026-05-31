/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.SinkProvidingStageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.InputSinkDecorator;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.analytics.spi.ReducingExchangeSink;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Coordinator-side reduce stage execution. Task invokes {@link ReducingExchangeSink#reduce};
 * {@link #onTerminalTransition} invokes {@link ExchangeSink#close} (idempotent) so
 * cancel-before-reduce paths still release resources. Scheduling mode (eager vs buffered)
 * is delegated to {@link ReducingExchangeSink#supportsEagerScheduling()}.
 *
 * <p>Dispatched on the scheduler pool (lightweight, handles wait/orchestration) which then
 * forks the actual reduce computation to the SEARCH pool. This prevents deadlocking SEARCH
 * (where fragment execution runs) while keeping reduce compute off the scheduler threads.
 *
 * @opensearch.internal
 */
public final class ReduceStageExecution extends AbstractStageExecution implements SinkProvidingStageExecution {

    private static final Logger logger = LogManager.getLogger(ReduceStageExecution.class);

    private final ReducingExchangeSink backendSink;
    private final ExchangeSink downstream;
    private final Executor reduceExecutor;
    private final BufferAllocator allocator;

    public ReduceStageExecution(Stage stage, QueryContext config, ReducingExchangeSink backendSink, ExchangeSink downstream) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.backendSink = backendSink;
        this.downstream = downstream;
        this.reduceExecutor = config.reduceExecutor();
        this.allocator = config.bufferAllocator();
        this.runner = new LocalTaskRunner(config.schedulerExecutor());
    }

    @Override
    public boolean schedulesEagerly() {
        return backendSink.supportsEagerScheduling();
    }

    @Override
    public void closeChildInput(int childStageId) {
        logger.info("[reduce-stage] closeChildInput: stageId={} childStageId={} isMultiInput={}",
            getStageId(), childStageId, backendSink instanceof MultiInputExchangeSink);
        if (backendSink instanceof MultiInputExchangeSink multi) {
            multi.sinkForChild(childStageId).close();
        }
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        InputSinkDecorator decorator = stage.getInputSinkDecorator();
        // sinkForChild routing only applies for Union/Join shapes with multiple child stages.
        if (stage.getChildStages().size() > 1) {
            if (decorator != null) {
                throw new IllegalStateException(
                    "InputSinkDecorator on a multi-input reducer (stageId=" + getStageId() + ") is not supported"
                );
            }
            return ((MultiInputExchangeSink) backendSink).sinkForChild(childStageId);
        }
        return decorator != null ? decorator.decorate(backendSink, allocator) : backendSink;
    }

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
    protected List<StageTask> materializeTasks() {
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), listener -> {
            logger.info("[reduce-stage] reduce task dispatched, stageId={}", getStageId());
            reduceExecutor.execute(() -> {
                try {
                    backendSink.reduce(listener);
                    logger.info("[reduce-stage] reduce() returned normally, stageId={}", getStageId());
                } catch (Exception e) {
                    logger.info("[reduce-stage] reduce() threw, stageId={}: {}", getStageId(), e.getMessage());
                    listener.onFailure(e);
                }
            });
        }));
    }

    @Override
    public boolean failWithCause(Exception cause) {
        logger.info("[reduce-stage] failWithCause: stageId={} cause={}", getStageId(),
            cause != null ? cause.getMessage() : "null");
        return super.failWithCause(cause);
    }

    @Override
    protected void onTerminalTransition(State terminal) {
        logger.info("[reduce-stage] onTerminalTransition: stageId={} terminal={}", getStageId(), terminal);
        try {
            backendSink.close();
        } catch (Exception ignore) {}
    }
}
