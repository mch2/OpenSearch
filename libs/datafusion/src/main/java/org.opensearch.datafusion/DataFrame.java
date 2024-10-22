/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * Represents a DataFrame, which is a result of a query.
 * This class provides methods to collect the DataFrame from the DataFusion runtime.
 */
public class DataFrame {

    public static Logger logger = LogManager.getLogger(DataFrame.class);

    long ptr;

    public DataFrame(long ptr) {
        this.ptr = ptr;
    }

    public CompletableFuture<ArrowReader> collect(SessionContext ctx, BufferAllocator allocator) {
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
}
