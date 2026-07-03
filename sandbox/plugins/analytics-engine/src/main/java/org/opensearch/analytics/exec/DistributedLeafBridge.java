/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.concurrent.ConcurrentMap;

/**
 * Engine-side implementation of the distributed-leaf bridge (Model B). The data-node Rust
 * {@code ShardScanExec} terminates a leaf task and upcalls Java (via FFM →
 * {@code LeafBridgeCallbacks} → backend {@code registerLeafBridge}) to set up the scan. This class
 * only resolves the local shard (it holds {@link IndicesService}) and delegates ALL
 * reader-acquisition + context + delegation + native-session setup to
 * {@link AnalyticsSearchService#openDistributedLeaf} — reusing the existing data-node logic rather
 * than reimplementing it (cases 1 &amp; 2: DataFusion executes parquet / indexed scans).
 *
 * <p>Case 3 (Lucene executes → rows / Arrow doc-values, pulled via a Java cursor) is a follow-on:
 * {@link #open} would return {@code JAVA_CURSOR} with a cursor over the existing
 * {@code Iterator<EngineResultBatch>}, pulled by {@link #next} / released by {@link #close}.
 *
 * <p>Registered once at node start via {@code backend.registerLeafBridge(this)}.
 */
public final class DistributedLeafBridge implements AnalyticsSearchBackendPlugin.LeafBridge {

    private static final Logger LOGGER = LogManager.getLogger(DistributedLeafBridge.class);

    /** Discriminators — MUST match {@code LeafBridgeCallbacks.LEAF_MODE_*} / the Rust constants. */
    private static final int LEAF_MODE_NATIVE = 1;
    private static final int LEAF_MODE_JAVA_CURSOR = 2;

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final AnalyticsSearchBackendPlugin backend;
    private final AnalyticsSearchService searchService;

    /** Live leaf handles keyed by the native session pointer, released when the leaf closes. */
    private final ConcurrentMap<Long, Releasable> openReaders = ConcurrentCollections.newConcurrentMap();

    public DistributedLeafBridge(
        IndicesService indicesService,
        ClusterService clusterService,
        AnalyticsSearchBackendPlugin backend,
        AnalyticsSearchService searchService
    ) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.backend = backend;
        this.searchService = searchService;
    }

    @Override
    public Opened open(long queryId, String indexUuid, int shardId, byte[] substrait, byte[] descriptor, int treeShape, int predicateCount)
        throws Exception {
        IndexShard shard = resolveShard(indexUuid, shardId);

        // Deserialize the delegation descriptor (empty = vanilla scan). The tree shape / predicate count
        // travel inside the descriptor, so treeShape/predicateCount args are advisory only.
        DelegationDescriptor delegation = null;
        if (descriptor != null && descriptor.length > 0) {
            try (org.opensearch.core.common.io.stream.StreamInput in = org.opensearch.core.common.io.stream.StreamInput.wrap(descriptor)) {
                delegation = new DelegationDescriptor(in);
            }
        }

        // Delegate ALL reader/context/delegation/native-session setup to AnalyticsSearchService's
        // existing logic (openDistributedLeaf reuses buildContext + the startFragment delegation block).
        // Nothing is reimplemented here; the bridge only resolves the shard and tracks the handle.
        AnalyticsSearchService.DistributedLeafHandle leaf = searchService.openDistributedLeaf(
            backend,
            shard,
            null, // Task — QTF/cancellation tracking is a follow-on for the distributed leaf
            queryId,
            substrait,
            delegation
        );
        long nativePtr = leaf.nativeSessionPtr();
        openReaders.put(nativePtr, leaf::close);
        LOGGER.debug(
            "openFragment NATIVE{}: query={} shard={} -> sessionPtr={}",
            delegation != null ? " (indexed)" : "",
            queryId,
            shard.shardId(),
            nativePtr
        );
        return new Opened(LEAF_MODE_NATIVE, nativePtr);
    }

    @Override
    public long next(long cursor) {
        // Only used for LEAF_MODE_JAVA_CURSOR (case 3), which open() does not yet return.
        throw new UnsupportedOperationException("JAVA_CURSOR leaf path (case 3) not yet implemented");
    }

    @Override
    public void close(long handle) {
        // Release the reader gate held open for this native session. The Rust leaf already owns +
        // drops the SessionContextHandle itself; here we just release the Java-side reader lease.
        Releasable r = openReaders.remove(handle);
        if (r != null) {
            try {
                r.close();
            } catch (Exception e) {
                LOGGER.warn("failed to release reader for leaf handle " + handle, e);
            }
        }
    }

    /** Map (indexUuid, shardId) → the local IndexShard, disambiguating same-numbered shards. */
    private IndexShard resolveShard(String indexUuid, int shardId) {
        for (IndexMetadata im : clusterService.state().metadata()) {
            if (im.getIndexUUID().equals(indexUuid)) {
                Index index = im.getIndex();
                return indicesService.indexServiceSafe(index).getShard(shardId);
            }
        }
        // Fallback: some callers pass the index name as uuid for single-index queries.
        ShardId sid = null;
        var lookup = clusterService.state().metadata().getIndicesLookup().get(indexUuid);
        if (lookup != null && lookup.getWriteIndex() != null) {
            sid = new ShardId(lookup.getWriteIndex().getIndex(), shardId);
        }
        if (sid != null) {
            return indicesService.indexServiceSafe(sid.getIndex()).getShard(sid.id());
        }
        throw new IllegalStateException("No local index with uuid/name [" + indexUuid + "] for shard " + shardId);
    }
}
