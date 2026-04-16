/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.internal;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Execution context for local stages. Carries the query identity,
 * the serialized fragment, and a shared Arrow allocator.
 * <p>
 * Moved from {@code org.opensearch.analytics.backend} (analytics-framework)
 * to the DataFusion backend module since it is now backend-internal.
 *
 * @opensearch.internal
 */
public class LocalExecutionContext {

    private final String queryId;
    private final int stageId;
    private final byte[] fragmentBytes;
    private final BufferAllocator allocator;

    public LocalExecutionContext(String queryId, int stageId, byte[] fragmentBytes, BufferAllocator allocator) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.fragmentBytes = fragmentBytes;
        this.allocator = allocator;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getStageId() {
        return stageId;
    }

    public byte[] getFragmentBytes() {
        return fragmentBytes;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }
}
