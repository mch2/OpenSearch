/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.MultiIndexShardScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Multi-index variant of {@link ShardScanInstructionHandler}. Registers the table under the
 * logical name (alias/pattern) and passes plan bytes so Rust can widen the schema for columns
 * this shard doesn't have.
 *
 * <p>TODO: Replace plan-bytes-based widening with an explicit Arrow IPC union schema once
 * OpenSearchSchemaBuilder and CoreDataFieldPlugin are consolidated into a shared type registry.
 */
public class MultiIndexShardScanInstructionHandler implements FragmentInstructionHandler<MultiIndexShardScanInstructionNode> {

    private final DataFusionPlugin plugin;

    MultiIndexShardScanInstructionHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        MultiIndexShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext context = (ShardScanExecutionContext) commonContext;
        DataFusionService dataFusionService = plugin.getDataFusionService();
        DataFormatRegistry registry = plugin.getDataFormatRegistry();

        DatafusionReader dfReader = null;
        for (String formatName : plugin.getSupportedFormats()) {
            dfReader = context.getReader().getReader(registry.format(formatName), DatafusionReader.class);
            if (dfReader != null) break;
        }
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }

        long readerPtr = dfReader.getReaderHandle().getPointer();
        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            SessionContextHandle sessionCtxHandle = NativeBridge.createMultiIndexSessionContext(
                readerPtr,
                runtimePtr,
                node.getLogicalTableName(),
                contextId,
                segment.address(),
                context.getFragmentBytes()
            );
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }
}
