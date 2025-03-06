/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Main DataFusion Entrypoint.
 */
public class DataFusion {
    public static Logger logger = LogManager.getLogger(DataFusion.class);
    static {
        System.loadLibrary("datafusion_jni");
    }

    // Coordinator methods -
    // creates a DataFrame from a list of tickets.
    static native void query(long runtime, long ctx, byte[] ticket, ObjectResultCallback callback);
    static native void agg(long runtime, long ctx, byte[] ticket, int size, ObjectResultCallback callback);

    // Data Node methods

    public static CompletableFuture<DataFrame> query(SessionContext ctx, byte[] ticket) {
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.query(ctx.getRuntime(), ctx.getPointer(), ticket, (err, ptr) -> {
            if (err != null) {
                future.completeExceptionally(new RuntimeException(err));
            } else {
                DataFrame df = new DataFrame(ctx, ptr);
                future.complete(df);
            }
        });
        return future;
    }

    public static CompletableFuture<DataFrame> agg(SessionContext ctx, byte[] ticket, int limit) {
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.agg(ctx.getRuntime(), ctx.getPointer(), ticket, limit, (err, ptr) -> {
            if (err != null) {
                future.completeExceptionally(new RuntimeException(err));
            } else {
                DataFrame df = new DataFrame(ctx, ptr);
                future.complete(df);
            }
        });
        return future;
    }
}
