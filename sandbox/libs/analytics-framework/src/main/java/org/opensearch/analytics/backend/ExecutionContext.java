/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

/**
 * Execution context carrying reader and plan state through
 * the query execution lifecycle.
 *
 * @opensearch.internal
 */
public class ExecutionContext {

    private final String tableName;
    private final Reader reader;
    private final SearchShardTask task;
    private byte[] fragmentBytes;
    private BufferAllocator allocator;

    /**
     * Constructs an execution context.
     * @param tableName the target table name
     * @param task the search shard task
     * @param reader the data-format aware reader
     */
    public ExecutionContext(String tableName, SearchShardTask task, Reader reader) {
        this.tableName = tableName;
        this.task = task;
        this.reader = reader;
    }

    /** Returns the search shard task. */
    public SearchShardTask getTask() {
        return task;
    }

    /** Returns the target table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the data-format aware reader. */
    public Reader getReader() {
        return reader;
    }

    /** Returns the backend-specific serialized plan fragment bytes, or null if not set. */
    public byte[] getFragmentBytes() {
        return fragmentBytes;
    }

    /** Sets the backend-specific serialized plan fragment bytes. */
    public void setFragmentBytes(byte[] fragmentBytes) {
        this.fragmentBytes = fragmentBytes;
    }

    /**
     * Returns the allocator to use as the parent when creating result-stream
     * buffers, or null if the backend should use its own default allocator.
     *
     * <p>Set to the transport channel's allocator for requests coming in over
     * Arrow Flight — producing buffers in the transport's allocator tree lets
     * the streaming framework do true zero-copy transfer into the wire-side
     * root without cross-tree accounting races.
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /** Sets the parent allocator for result-stream buffer allocations. */
    public void setAllocator(BufferAllocator allocator) {
        this.allocator = allocator;
    }
}
