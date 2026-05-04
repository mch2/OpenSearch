/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * Represents a DataFusion query — wraps substrait plan bytes and execution metadata.
 */
public class DatafusionQuery {

    private final String indexName;
    private final byte[] substraitBytes;
    private boolean fetchPhase;
    private long contextId;
    private final boolean singleShard;

    /**
     * Creates a query with the given index name and serialized substrait plan.
     * @param indexName the target index name
     * @param substraitBytes the serialized substrait plan bytes
     * @param contextId the query context ID for per-query memory tracking (0 if unavailable)
     * @param singleShard true when the index has a single shard, so the data node runs the
     *                    aggregate end-to-end (no coordinator merge will follow). Native side
     *                    skips the partial/final mode rewrite when this is true.
     */
    public DatafusionQuery(String indexName, byte[] substraitBytes, long contextId, boolean singleShard) {
        this.indexName = indexName;
        this.substraitBytes = substraitBytes;
        this.contextId = contextId;
        this.singleShard = singleShard;
    }

    /** Returns the target index name. */
    public String getIndexName() {
        return indexName;
    }

    /** Returns the serialized substrait plan bytes. */
    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    /** Returns whether this query is in the fetch phase. */
    public boolean isFetchPhase() {
        return fetchPhase;
    }

    /**
     * Sets whether this query is in the fetch phase.
     * @param fetchPhase true if this query is in the fetch phase
     */
    public void setFetchPhase(boolean fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    /** Returns the query context ID for per-query memory tracking. */
    public long getContextId() {
        return contextId;
    }

    /** Returns true when the index has a single shard — the data node runs the aggregate
     *  end-to-end and the native executor must not rewrite aggregates to {@code Partial}. */
    public boolean isSingleShard() {
        return singleShard;
    }
}
