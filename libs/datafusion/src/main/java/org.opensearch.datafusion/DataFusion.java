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

    // collect the DataFrame
    static native void collect(long runtime, long df, BiConsumer<String, byte[]> callback);

    public static CompletableFuture<DataFrame> doQuery(SessionContext ctx, List<byte[]> tickets) {
        CompletableFuture<DataFrame> future = new CompletableFuture<>();
        DataFusion.query(ctx.getRuntime(), ctx.getPointer(), tickets, (err, ptr) -> {
            if (err != null) {
                future.completeExceptionally(new RuntimeException(err));
            } else {
                DataFrame df = new DataFrame(ptr);
                future.complete(df);
            }
        });
        return future;
    }
}
