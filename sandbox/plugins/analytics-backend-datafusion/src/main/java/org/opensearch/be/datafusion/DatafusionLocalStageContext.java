/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.be.datafusion.internal.InputHandle;
import org.opensearch.be.datafusion.internal.LocalExecutionContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DataFusion-backed {@link LocalStageContext} implementation.
 * <p>
 * Internally constructs a {@link DatafusionLocalExecEngine}, registers one
 * input per child stage, creates a {@link DatafusionChildSink} per child,
 * and starts engine execution. The walker feeds child stage output into the
 * per-child sinks; {@link #asyncFinalize} closes inputs, drains the engine
 * output, and forwards drained batches to the downstream sink.
 *
 * @opensearch.internal
 */
class DatafusionLocalStageContext implements LocalStageContext {

    private static final Logger logger = LogManager.getLogger(DatafusionLocalStageContext.class);

    private final DatafusionLocalExecEngine engine;
    private final Map<Integer, InputHandle> handles;
    private final Map<Integer, ExchangeSink> childSinks;
    private final ExchangeSink downstream;
    private final BufferAllocator allocator;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile Thread drainThread;
    private final EngineResultStream output;
    private final String queryId;
    private final int stageId;

    DatafusionLocalStageContext(LocalStageRequest req, NativeRuntimeHandle runtimeHandle) {
        this.queryId = req.getQueryId();
        this.stageId = req.getStageId();
        this.allocator = req.getAllocator();
        this.downstream = req.getDownstream();
        this.handles = new HashMap<>();
        this.childSinks = new HashMap<>();

        // Build a LocalExecutionContext for the existing DatafusionLocalExecEngine
        LocalExecutionContext ctx = new LocalExecutionContext(
            req.getQueryId(),
            req.getStageId(),
            req.getFragmentBytes(),
            req.getAllocator()
        );
        this.engine = new DatafusionLocalExecEngine(ctx, runtimeHandle);

        // Register one input per child stage and create per-child sinks
        for (Map.Entry<Integer, Schema> entry : req.getChildSchemas().entrySet()) {
            int childStageId = entry.getKey();
            Schema schema = entry.getValue();
            String inputId = stageInputId(childStageId);
            DatafusionInputHandle handle = engine.registerInput(inputId, schema);
            handles.put(childStageId, handle);
            childSinks.put(childStageId, new DatafusionChildSink(handle, schema, allocator, childStageId));
            logger.info("[DF.LocalStageContext] registered child childStageId={} inputId={} schema={}", childStageId, inputId, schema);
        }

        // Start engine execution — begins polling inputs (which initially block on empty mpsc channels)
        this.output = engine.execute();

        logger.info("[DF.LocalStageContext] CREATED queryId={} stageId={} children={}", queryId, stageId, req.getChildSchemas().keySet());
    }

    @Override
    public ExchangeSink sinkFor(int childStageId) {
        ExchangeSink sink = childSinks.get(childStageId);
        if (sink == null) {
            throw new IllegalArgumentException("No sink registered for child stage " + childStageId);
        }
        logger.info("[DF.LocalStageContext] sinkFor childStageId={}", childStageId);
        return sink;
    }

    @Override
    public void asyncFinalize(ActionListener<Void> listener) {
        logger.info("[DF.LocalStageContext] asyncFinalize ENTRY stageId={}", stageId);

        // Close all inputs to signal EOF to the engine
        handles.values().forEach(InputHandle::closeInput);

        // Start the drain on a virtual thread
        Thread.ofVirtual().name("datafusion-local-drain-" + stageId).start(() -> {
            drainThread = Thread.currentThread();
            int batchCount = 0;
            long totalRows = 0;
            logger.info("[DF.LocalStageContext] drain STARTED stageId={} thread={}", stageId, Thread.currentThread());
            try {
                Iterator<EngineResultBatch> it = output.iterator();
                while (cancelled.get() == false && it.hasNext()) {
                    EngineResultBatch batch = it.next();
                    batchCount++;
                    totalRows += batch.getRowCount();
                    logger.info(
                        "[DF.LocalStageContext] drain received batch #{} rows={} fields={} stageId={}",
                        batchCount,
                        batch.getRowCount(),
                        batch.getFieldNames(),
                        stageId
                    );
                    FragmentExecutionResponse response = batchToResponse(batch);
                    synchronized (downstream) {
                        downstream.feed(response);
                    }
                }
                if (cancelled.get()) {
                    logger.info("[DF.LocalStageContext] drain CANCELLED stageId={} batchesDrained={}", stageId, batchCount);
                    listener.onFailure(new TaskCancelledException("local stage cancelled"));
                    return;
                }
                output.close();
                logger.info(
                    "[DF.LocalStageContext] drain COMPLETE stageId={} totalBatches={} totalRows={}",
                    stageId,
                    batchCount,
                    totalRows
                );
                close();
                logger.info("[DF.LocalStageContext] finalize signaled SUCCESS stageId={}", stageId);
                listener.onResponse(null);
            } catch (Exception e) {
                if (cancelled.get() || e instanceof InterruptedException) {
                    logger.info("[DF.LocalStageContext] drain interrupted/cancelled stageId={}", stageId);
                    listener.onFailure(new TaskCancelledException("local stage cancelled"));
                } else {
                    logger.warn(
                        "[DF.LocalStageContext] drain FAILED stageId={} batchesBeforeFailure={}: {}",
                        stageId,
                        batchCount,
                        e.getMessage()
                    );
                    close();
                    listener.onFailure(e);
                }
            } finally {
                drainThread = null;
            }
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cancelled.set(true);
            logger.info("[DF.LocalStageContext] close() stageId={}", stageId);
            Thread t = drainThread;
            if (t != null) {
                t.interrupt();
            }
            try {
                engine.close();
            } catch (Exception ignore) {}
        }
    }

    /**
     * Converts an {@link EngineResultBatch} to a {@link FragmentExecutionResponse}.
     */
    static FragmentExecutionResponse batchToResponse(EngineResultBatch batch) {
        List<String> fieldNames = batch.getFieldNames();
        List<Object[]> rows = new ArrayList<>();
        for (int r = 0; r < batch.getRowCount(); r++) {
            Object[] row = new Object[fieldNames.size()];
            for (int c = 0; c < fieldNames.size(); c++) {
                row[c] = batch.getFieldValue(fieldNames.get(c), r);
            }
            rows.add(row);
        }
        return new FragmentExecutionResponse(fieldNames, rows);
    }

    static String stageInputId(int childStageId) {
        return "__stage_" + childStageId + "_input__";
    }
}
