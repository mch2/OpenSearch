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
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.opensearch.datafusion.DataFrame;
import org.opensearch.datafusion.ObjectResultCallback;
import org.opensearch.datafusion.SessionContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DataFusionAggregator implements AutoCloseable {
    static {
        System.loadLibrary("datafusion_jni");
    }
    private final SessionContext context;
    private final long ptr;
    private final DictionaryProvider dictionaryProvider;
    private final String term;

    public DataFusionAggregator(String term, int batchSize) {
        this.context = new SessionContext(batchSize);
        this.ptr = create(context.getPointer(), term);
        this.term = term;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    // use default DF batch size (8192)
    public DataFusionAggregator(String term) {
        this.context = new SessionContext();
        this.ptr = create(context.getPointer(), term);
        this.dictionaryProvider = new CDataDictionaryProvider();
        this.term = term;
    }

    public CompletableFuture<DataFrame> pushBatch(BufferAllocator allocator, VectorSchemaRoot root) {
        CompletableFuture<DataFrame> result = new CompletableFuture<>();

        try {
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator);

            Data.exportVectorSchemaRoot(allocator, root, dictionaryProvider, array, schema);

            processBatch(
                context.getRuntime(),
                context.getPointer(),
                array.memoryAddress(),
                schema.memoryAddress(),
                this.term,
                (String errString, long ptr) -> {
                    if (errString != null && !errString.isEmpty()) {
                        result.completeExceptionally(new RuntimeException(errString));
                    } else {
                        DataFrame df = new DataFrame(context, ptr);
                        result.complete(df);
                    }
                });
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    public CompletableFuture<DataFrame> getResults(int limit) {
        // limit currently ignored in DF.
        CompletableFuture<DataFrame> result = new CompletableFuture<>();
        getResults(
            context.getRuntime(),
            ptr,
            limit,
            (String errString, long ptr) -> {
                if (errString != null && !errString.isEmpty()) {
                    result.completeExceptionally(new RuntimeException(errString));
                } else if (ptr == 0) {
                    result.complete(null);
                } else {
                    result.complete(new DataFrame(context, ptr));
                }
            });
        return result;
    }

    @Override
    public void close() {
        destroy(ptr);
    }

    private static native void processBatch(long runtime, long ctx, long arrayPtr, long schemaPtr, String term, ObjectResultCallback callback);
    private static native long create(long ctx, String term);
    private static native void pushBatch(long runtime, long ptr, long schema, long array, ObjectResultCallback callback);
    private static native void getResults(long runtime, long ptr, int limit, ObjectResultCallback callback);
    private static native void destroy(long ptr);
    private static native void unionFrames(long runtime, long ctx, long[] framePointers, int limit, ObjectResultCallback callback);

    public CompletableFuture<DataFrame> getResults(List<DataFrame> frames, int limit) {
        CompletableFuture<DataFrame> result = new CompletableFuture<>();

        try {
            // Convert frames to array of pointers
            long[] framePointers = new long[frames.size()];
            for (int i = 0; i < frames.size(); i++) {
                framePointers[i] = frames.get(i).getPtr();
            }

            // Call native method
            unionFrames(
                context.getRuntime(),
                context.getPointer(),
                framePointers,
                limit,
                (String errString, long ptr) -> {
                    if (errString != null && !errString.isEmpty()) {
                        result.completeExceptionally(new RuntimeException(errString));
                    } else if (ptr == 0) {
                        result.complete(null);
                    } else {
                        result.complete(new DataFrame(context, ptr, frames));
                    }
                }
            );
        } catch (Exception e) {
            result.completeExceptionally(e);
        }

        return result;
    }
}
