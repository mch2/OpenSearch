/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
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

    // create a DataFrame from a list of tickets.
    static native void query(long runtime, long ctx, byte[] ticket, ObjectResultCallback callback);

    static native void agg(long runtime, long ctx, byte[] ticket, ObjectResultCallback callback);

    // collect the DataFrame
    static native void collect(long runtime, long df, BiConsumer<String, byte[]> callback);

    static native void executeStream(long runtime, long dataframe, ObjectResultCallback callback);

    public static CompletableFuture<DataFrame> query(byte[] ticket) {
        SessionContext ctx = new SessionContext();
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

    public static CompletableFuture<DataFrame> agg(byte[] ticket) {
        SessionContext ctx = new SessionContext();
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.agg(ctx.getRuntime(), ctx.getPointer(), ticket, (err, ptr) -> {
            if (err != null) {
                future.completeExceptionally(new RuntimeException(err));
            } else {
                DataFrame df = new DataFrame(ctx, ptr);
                logger.info("Returning DataFrame ref from jni");
                future.complete(df);
            }
        });
        return future;
    }
}
