/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * DataFusion-backed search execution engine.
 * <p>
 * Delegates execution to the native DataFusion runtime via {@link DatafusionSearcher}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearchExecEngine implements SearchExecEngine<ExecutionContext, EngineResultStream> {

    private final DatafusionContext datafusionContext;
    private final Supplier<BufferAllocator> allocatorFactory;

    /**
     * Creates an execution engine backed by the given DataFusion context.
     * @param datafusionContext the DataFusion execution context
     * @param allocatorFactory factory for creating a child allocator for result stream memory
     */
    public DatafusionSearchExecEngine(DatafusionContext datafusionContext, Supplier<BufferAllocator> allocatorFactory) {
        this.datafusionContext = datafusionContext;
        this.allocatorFactory = allocatorFactory;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        byte[] substraitBytes = requestContext.getFragmentBytes();
        datafusionContext.setDatafusionQuery(new DatafusionQuery(requestContext.getTableName(), substraitBytes));
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        DatafusionSearcher searcher = datafusionContext.getSearcher();
        searcher.search(datafusionContext);
        StreamHandle handle = datafusionContext.takeStreamHandle();
        // If the caller supplies an allocator (e.g. Flight's channel allocator), create
        // a child from it so the stream's buffers live in the caller's tree — keeps
        // transferTo() truly zero-copy and avoids cross-tree close races. Otherwise
        // fall back to the backend's own root.
        BufferAllocator parent = requestContext.getAllocator();
        BufferAllocator allocator = parent != null
            ? parent.newChildAllocator("datafusion-stream", 0, Long.MAX_VALUE)
            : allocatorFactory.get();
        return new DatafusionResultStream(handle, allocator);
    }

    @Override
    public void close() throws IOException {
        datafusionContext.close();
    }
}
