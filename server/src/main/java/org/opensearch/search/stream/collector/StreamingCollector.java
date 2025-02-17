/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream.collector;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.datafusion.DataFrame;
import org.opensearch.datafusion.SessionContext;

import java.util.List;

public class StreamingCollector implements AutoCloseable {
    private final SessionContext context;
    private final BufferAllocator allocator;
    private VectorSchemaRoot currentRoot;
    private final Object lock = new Object();
    private boolean finished = false;
    private int batchSize;
    private TimeValue waitTimeout;
    private DataFrame dataFrame;

    public StreamingCollector(
        SessionContext context,
        BufferAllocator allocator,
        VectorSchemaRoot root,
        int batchSize,
        TimeValue waitTimeout,
        String term,
        int limit) {
        this.context = context;
        this.allocator = allocator;
        this.currentRoot = root;
        this.batchSize = batchSize;
        this.waitTimeout = waitTimeout;
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        try {
            Data.exportSchema(allocator, root.getSchema(), new CDataDictionaryProvider(), schema);
            this.dataFrame = new DataFrame(context, create(context.getPointer(), this, limit, term, schema.memoryAddress()));
        } finally {
            schema.close();
        }
    }

    public DataFrame getResults() {
        synchronized (lock) {
            finish();
            return dataFrame;
        }
    }

    // Inner class to hold both pointers
    public static class BatchPointers {
        public long schemaPtr;
        public long arrayPtr;

        public BatchPointers(long schemaPtr, long arrayPtr) {
            this.schemaPtr = schemaPtr;
            this.arrayPtr = arrayPtr;
        }
    }

    // Called by DataFusion through JNI
    public BatchPointers getNextBatch() {
        synchronized (lock) {
            if (finished && currentRoot.getRowCount() == 0) {
                return null;
            }

            if (currentRoot.getRowCount() > 0) {
                VectorSchemaRoot batchToReturn = currentRoot;
                ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema schema = ArrowSchema.allocateNew(allocator);

                try {
                    Data.exportVectorSchemaRoot(allocator, batchToReturn, new CDataDictionaryProvider(), array, schema);
                    return new BatchPointers(schema.memoryAddress(), array.memoryAddress());
                } catch (Exception e) {
                    // Handle any export errors
                    array.close();
                    schema.close();
                    return null;
                }
            }

            try {
                // Wait for more data with timeout
                lock.wait(waitTimeout.getMillis());
                return getNextBatch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    // Called by ArrowCollector when a batch is ready
    public void offerBatch(VectorSchemaRoot batch) {
        synchronized (lock) {
            this.currentRoot = batch;
            lock.notify();
        }
    }

    public void finish() {
        synchronized (lock) {
            finished = true;
            lock.notify();
        }
    }

    @Override
    public void close() {
        if (currentRoot != null) {
            currentRoot.close();
        }
        try {
            dataFrame.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static native long create(long ctx, Object collector, int limit, String term, long schema);
}
