/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.internal.InputHandle;

/**
 * {@link InputHandle} implementation backed by FFM downcalls to the DataFusion
 * native library. Each instance wraps a sender handle returned by
 * {@link FfmBindings#createPartitionStream}.
 * <p>
 * {@link #pushBatch(VectorSchemaRoot)} exports the VSR via the Arrow C Data Interface
 * and passes the raw addresses to the Rust side. Ownership of the batch transfers
 * to the engine — the caller must not close the batch after pushing.
 */
class DatafusionInputHandle implements InputHandle {

    private static final Logger logger = LogManager.getLogger(DatafusionInputHandle.class);

    private final long senderHandle;
    private final BufferAllocator allocator;
    private volatile boolean closed = false;

    DatafusionInputHandle(long senderHandle, BufferAllocator allocator) {
        this.senderHandle = senderHandle;
        this.allocator = allocator;
    }

    @Override
    public void pushBatch(VectorSchemaRoot batch) {
        if (closed) {
            throw new IllegalStateException("pushBatch called after closeInput");
        }
        int rowCount = batch.getRowCount();
        logger.info("[DF.InputHandle] pushBatch ENTRY senderHandle={} rows={}", senderHandle, rowCount);
        // Export via Arrow C Data Interface — ArrowArray/ArrowSchema allocate
        // native memory compatible with the C Data Interface. We pass their
        // raw addresses as longs through FFM.
        try (ArrowArray array = ArrowArray.allocateNew(allocator); ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(allocator, batch, null, array, schema);
            FfmBindings.pushBatch(senderHandle, array.memoryAddress(), schema.memoryAddress());
            logger.info("[DF.InputHandle] pushBatch native call returned senderHandle={} rows={}", senderHandle, rowCount);
        }
        // Ownership transferred to Rust; release Java-side vectors.
        batch.close();
    }

    @Override
    public void closeInput() {
        if (closed) {
            return;
        }
        closed = true;
        logger.info("[DF.InputHandle] closeInput senderHandle={}", senderHandle);
        FfmBindings.closePartitionStream(senderHandle);
    }
}
