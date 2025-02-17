/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a DataFrame, which is a result of a query.
 * This class provides methods to collect the DataFrame from the DataFusion runtime.
 */
public class DataFrame implements AutoCloseable {

    public static Logger logger = LogManager.getLogger(DataFrame.class);
    private final SessionContext ctx;

    long ptr;

    static native void destroyDataFrame(long pointer);

    public DataFrame(SessionContext ctx, long ptr) {
        this.ctx = ctx;
        this.ptr = ptr;
    }

    public CompletableFuture<ArrowReader> collect(BufferAllocator allocator) {
        CompletableFuture<ArrowReader> result = new CompletableFuture<>();
        DataFusion.collect(ctx.getRuntime(), ptr, (err, obj) -> {
            if (err != null && err.isEmpty() == false) {
                result.completeExceptionally(new RuntimeException(err));
            } else {
                result.complete(new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(obj), allocator));
            }
        });
        return result;
    }

    // return a stream over the dataframe
    public CompletableFuture<RecordBatchStream> getStream(BufferAllocator allocator) {
        CompletableFuture<RecordBatchStream> result = new CompletableFuture<>();
        long runtimePointer = ctx.getRuntime();
        DataFusion.executeStream(runtimePointer, ptr, (String errString, long streamId) -> {
            if (errString != null && errString.isEmpty() == false) {
                result.completeExceptionally(new RuntimeException(errString));
            } else {
                result.complete(new RecordBatchStream(ctx, streamId, allocator));
            }
        });
        return result;
    }

    public SessionContext context() {
        return ctx;
    }

    @Override
    public void close() throws Exception {
        destroyDataFrame(ptr);
        ctx.close();
    }
}
