/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.be.datafusion.internal.InputHandle;

import java.util.List;

/**
 * Per-child {@link ExchangeSink} that receives Arrow
 * {@link VectorSchemaRoot} batches and pushes them into the
 * corresponding {@link InputHandle} (which feeds a DataFusion
 * partition stream via mpsc channel).
 *
 * <p>Closing the {@link InputHandle} (EOF) is NOT this sink's responsibility —
 * {@link DatafusionLocalStageContext#asyncFinalize} handles that.
 *
 * @opensearch.internal
 */
class DatafusionChildSink implements ExchangeSink {

    private static final Logger logger = LogManager.getLogger(DatafusionChildSink.class);

    private final InputHandle handle;
    private final Schema schema;
    private final BufferAllocator allocator;
    private final int childStageId;

    DatafusionChildSink(InputHandle handle, Schema schema, BufferAllocator allocator, int childStageId) {
        this.handle = handle;
        this.schema = schema;
        this.allocator = allocator;
        this.childStageId = childStageId;
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        int rowCount = batch.getRowCount();
        logger.info(
            "[DF.LocalStageContext] childSink.feed ENTRY childStageId={} rows={} fields={}",
            childStageId,
            rowCount,
            batch.getSchema().getFields()
        );
        handle.pushBatch(batch);
        logger.info("[DF.LocalStageContext] childSink.feed EXIT childStageId={} rows={}", childStageId, rowCount);
    }

    @Override
    public void close() {
        // no-op: input handle close happens in asyncFinalize
    }
}
