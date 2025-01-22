/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Main DataFusion Entrypoint.
 */
public class DataFusion {

    static {
        System.loadLibrary("datafusion_jni");
    }

    // create a DataFrame from a list of tickets.
    static native void query(long runtime, long ctx, List<byte[]> tickets, ObjectResultCallback callback);

    static native void join(long runtime, long ctx, String joinField, List<byte[]> left, List<byte[]> right, ObjectResultCallback callback);

    // collect the DataFrame
    static native void collect(long runtime, long df, BiConsumer<String, byte[]> callback);

    static native void executeStream(long runtime, long dataframe, ObjectResultCallback callback);

    public static CompletableFuture<DataFrame> query(List<byte[]> tickets) {
        SessionContext ctx = new SessionContext();
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.query(ctx.getRuntime(), ctx.getPointer(), tickets, (err, ptr) -> {
            if (err != null) {
                future.completeExceptionally(new RuntimeException(err));
            } else {
                DataFrame df = new DataFrame(ctx, ptr);
                future.complete(df);
            }
        });
        return future;
    }

    public static CompletableFuture<DataFrame> join(List<byte[]> left, List<byte[]> right, String joinField) {
        SessionContext ctx = new SessionContext();
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.join(ctx.getRuntime(), ctx.getPointer(), joinField, left, right, (err, ptr) -> {
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
