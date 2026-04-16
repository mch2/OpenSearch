/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.core.action.ActionListener;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only {@link LocalStageContext} that records all fed batches per child
 * and produces a summary output on asyncFinalize.
 *
 * <p>Used by {@code LocalStageDispatchIT} to verify the local stage dispatch
 * plumbing without requiring a real DataFusion backend.
 */
public class TestSummingLocalStageContext implements LocalStageContext {

    private final Map<Integer, List<VectorSchemaRoot>> received = new ConcurrentHashMap<>();
    private final ExchangeSink downstream;
    private final BufferAllocator allocator;
    private final AtomicInteger closeCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Thread drainThread;

    public TestSummingLocalStageContext(LocalStageRequest req) {
        this.downstream = req.getDownstream();
        this.allocator = req.getAllocator();
        for (Integer childId : req.getChildSchemas().keySet()) {
            received.put(childId, new CopyOnWriteArrayList<>());
        }
    }

    @Override
    public ExchangeSink sinkFor(int childStageId) {
        List<VectorSchemaRoot> bucket = received.get(childStageId);
        if (bucket == null) {
            throw new IllegalArgumentException("No sink registered for child stage " + childStageId);
        }
        return new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {
                bucket.add(batch);
            }

            @Override
            public void close() {}

            @Override
            public Iterable<Object[]> readResult() {
                return List.of();
            }

            @Override
            public long getRowCount() {
                return 0;
            }

            @Override
            public Object getValueAt(String column, int rowIndex) {
                return null;
            }
        };
    }

    @Override
    public void asyncFinalize(ActionListener<Void> listener) {
        Thread.ofVirtual().name("test-local-drain").start(() -> {
            drainThread = Thread.currentThread();
            try {
                // Build summary response
                int totalBatches = totalBatchesReceived();
                int totalRows = 0;
                for (List<VectorSchemaRoot> batches : received.values()) {
                    for (VectorSchemaRoot vsr : batches) {
                        totalRows += vsr.getRowCount();
                    }
                }
                int numInputs = received.size();

                // Build a VectorSchemaRoot summary
                List<Field> fields = List.of(
                    new Field("num_inputs", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    new Field("total_batches", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                    new Field("total_rows", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
                );
                Schema schema = new Schema(fields);
                VectorSchemaRoot summary = VectorSchemaRoot.create(schema, allocator);
                summary.allocateNew();
                ((VarCharVector) summary.getVector(0)).setSafe(0, String.valueOf(numInputs).getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) summary.getVector(1)).setSafe(0, String.valueOf(totalBatches).getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) summary.getVector(2)).setSafe(0, String.valueOf(totalRows).getBytes(StandardCharsets.UTF_8));
                summary.getVector(0).setValueCount(1);
                summary.getVector(1).setValueCount(1);
                summary.getVector(2).setValueCount(1);
                summary.setRowCount(1);

                downstream.feed(summary);
                close();
                listener.onResponse(null);
            } catch (Exception e) {
                close();
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeCount.incrementAndGet();
        }
    }

    // ---- Test inspection methods ----

    public int totalBatchesReceived() {
        return received.values().stream().mapToInt(List::size).sum();
    }

    public int batchesForInput(String stageInputId) {
        for (Map.Entry<Integer, List<VectorSchemaRoot>> entry : received.entrySet()) {
            String expectedId = "__stage_" + entry.getKey() + "_input__";
            if (expectedId.equals(stageInputId)) {
                return entry.getValue().size();
            }
        }
        return 0;
    }

    public boolean allInputsClosed() {
        return closed.get();
    }

    public int closeCount() {
        return closeCount.get();
    }

    public Thread drainThread() {
        return drainThread;
    }

    /** Releases all Arrow batches held by this context. */
    public void releaseAllBatches() {
        for (List<VectorSchemaRoot> batches : received.values()) {
            for (VectorSchemaRoot vsr : batches) {
                vsr.close();
            }
            batches.clear();
        }
    }
}
