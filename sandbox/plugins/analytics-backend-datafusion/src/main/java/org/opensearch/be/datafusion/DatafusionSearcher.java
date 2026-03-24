/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.ReaderHandle;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * DataFusion searcher — executes substrait query plans against a native DataFusion reader.
 * <p>
 * After {@link #search}, the result stream handle is available on the context
 * via {@link DatafusionContext#getStreamHandle()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionSearcher implements EngineSearcher<DatafusionContext> {

    private final ReaderHandle readerHandle;

    public DatafusionSearcher(ReaderHandle readerHandle) {
        this.readerHandle = readerHandle;
    }

    @Override
    public void search(DatafusionContext context) throws IOException {
        DatafusionQuery query = context.getDatafusionQuery();
        if (query == null) {
            throw new IllegalStateException("DatafusionQuery must be set before search");
        }
        long streamPtr = NativeBridge.executeQuery(
            readerHandle.getPointer(),
            query.getIndexName(),
            query.getSubstraitBytes(),
            context.getRuntimePtr()
        );
        context.setStreamHandle(new StreamHandle(streamPtr, context.getRuntimePtr()));
    }

    @Override
    public void close() {
        // ReaderHandle lifecycle is owned by DatafusionReader / EngineReaderManager,
        // not by the searcher. Do not close it here.
    }
}
