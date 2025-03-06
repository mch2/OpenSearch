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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DataFusionAggregator implements AutoCloseable {
    static {
        System.loadLibrary("datafusion_jni");
    }
    private final SessionContext context;
    private final DictionaryProvider dictionaryProvider;
    private final String term;
    List<CompletableFuture<DataFrame>> batchFutures = new ArrayList<>();

    public DataFusionAggregator(String term, int batchSize) {
        this.context = new SessionContext(batchSize);
        this.term = term;
        this.dictionaryProvider = new CDataDictionaryProvider();
    }

    // use default DF batch size (8192)
    public DataFusionAggregator(String term) {
        this.context = new SessionContext();
        this.dictionaryProvider = new CDataDictionaryProvider();
        this.term = term;
    }

    public CompletableFuture<DataFrame> exportBatch(BufferAllocator allocator, VectorSchemaRoot root) {
        CompletableFuture<DataFrame> result = new CompletableFuture<>();
        batchFutures.add(result);
        try (
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator)
        ) {
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

    public CompletableFuture<DataFrame> getResults() {
        CompletableFuture<DataFrame> result = new CompletableFuture<>();
        CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0])).thenAccept(a -> {
            List<DataFrame> frames = new ArrayList<>(batchFutures.size());
            for (CompletableFuture<DataFrame> future : batchFutures) {
                frames.add(future.join());
            }

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
                    500,
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
        });
        return result;
    }

    @Override
    public void close() throws Exception {
        context.close();
    }

    private static native void processBatch(long runtime, long ctx, long arrayPtr, long schemaPtr, String term, ObjectResultCallback callback);
    private static native void unionFrames(long runtime, long ctx, long[] framePointers, int limit, ObjectResultCallback callback);
}
