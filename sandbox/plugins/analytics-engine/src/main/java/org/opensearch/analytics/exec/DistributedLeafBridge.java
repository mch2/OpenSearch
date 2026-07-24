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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Engine-side implementation of the distributed-leaf bridge (Model B). The data-node Rust
 * {@code ShardScanExec} terminates a leaf task and upcalls Java (via FFM →
 * {@code LeafBridgeCallbacks} → backend {@code registerLeafBridge}) to set up the scan. This class
 * only resolves the local shard (it holds {@link IndicesService}) and delegates ALL
 * reader-acquisition + context + delegation + native-session setup to
 * {@link AnalyticsSearchService#openDistributedLeaf} — reusing the existing data-node logic rather
 * than reimplementing it (cases 1 &amp; 2: DataFusion executes parquet / indexed scans).
 *
 * <p>Case 3 (Lucene executes → Arrow doc-values, pulled via a Java cursor): selected when the
 * shard's PRIMARY data format is lucene (doc-values-backed analytics index — no parquet in the read
 * path). {@link #open} returns {@code JAVA_CURSOR} over a
 * {@link AnalyticsSearchBackendPlugin.LeafCursor}; the Rust {@code JavaCursorStream} pulls batches
 * via {@link #next} and releases via {@link #close} on drop.
 *
 * <p>Registered once at node start via {@code backend.registerLeafBridge(this)}.
 */
public final class DistributedLeafBridge implements AnalyticsSearchBackendPlugin.LeafBridge {

    private static final Logger LOGGER = LogManager.getLogger(DistributedLeafBridge.class);

    /** Discriminators — MUST match {@code LeafBridgeCallbacks.LEAF_MODE_*} / the Rust constants. */
    private static final int LEAF_MODE_NATIVE = 1;
    private static final int LEAF_MODE_JAVA_CURSOR = 2;

    /** {@code index.composite.primary_data_format} value that routes a leaf to the DV cursor. */
    private static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";
    private static final String LUCENE_FORMAT = "lucene";

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final AnalyticsSearchBackendPlugin backend;
    private final AnalyticsSearchService searchService;
    /** The accepting backend for the doc-values cursor (lucene), or null if not registered. */
    private final AnalyticsSearchBackendPlugin luceneBackend;

    /** Live leaf handles keyed by the native session pointer, released when the leaf closes. */
    private final ConcurrentMap<Long, Releasable> openReaders = ConcurrentCollections.newConcurrentMap();

    /** Live JAVA_CURSOR leaves keyed by a negative cursor id (disjoint from native session ptrs). */
    private final ConcurrentMap<Long, AnalyticsSearchBackendPlugin.LeafCursor> openCursors = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong cursorSeq = new AtomicLong(0L);

    public DistributedLeafBridge(
        IndicesService indicesService,
        ClusterService clusterService,
        AnalyticsSearchBackendPlugin backend,
        AnalyticsSearchService searchService
    ) {
        this(indicesService, clusterService, backend, searchService, null);
    }

    public DistributedLeafBridge(
        IndicesService indicesService,
        ClusterService clusterService,
        AnalyticsSearchBackendPlugin backend,
        AnalyticsSearchService searchService,
        AnalyticsSearchBackendPlugin luceneBackend
    ) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.backend = backend;
        this.searchService = searchService;
        this.luceneBackend = luceneBackend;
    }

    @Override
    public Opened open(
        long queryId,
        String indexUuid,
        int shardId,
        byte[] substrait,
        byte[] descriptor,
        int treeShape,
        int predicateCount,
        long arrowSchemaPtr
    ) throws Exception {
        IndexShard shard = resolveShard(indexUuid, shardId);

        // Deserialize the delegation descriptor (empty = vanilla scan). The tree shape / predicate count
        // travel inside the descriptor, so treeShape/predicateCount args are advisory only.
        DelegationDescriptor delegation = null;
        if (descriptor != null && descriptor.length > 0) {
            try (org.opensearch.core.common.io.stream.StreamInput in = org.opensearch.core.common.io.stream.StreamInput.wrap(descriptor)) {
                delegation = new DelegationDescriptor(in);
            }
        }

        // Case 3: doc-values-backed index (lucene primary) → Lucene scans + Java decodes; the Rust
        // leaf pulls Arrow batches from the returned cursor. No parquet/native session involved.
        if (isDocValuesPrimary(shard)) {
            if (luceneBackend == null) {
                throw new IllegalStateException(
                    "shard " + shard.shardId() + " is doc-values-backed (lucene primary) but the lucene backend is not registered"
                );
            }
            AnalyticsSearchBackendPlugin.LeafCursor cursor = searchService.openDocValuesLeaf(
                luceneBackend,
                shard,
                null, // Task — cancellation rides the gRPC stream drop → leaf_close, like the native leaf
                substrait,
                delegation,
                arrowSchemaPtr
            );
            long cursorId = cursorSeq.decrementAndGet();
            openCursors.put(cursorId, cursor);
            LOGGER.debug(
                "openFragment JAVA_CURSOR{}: query={} shard={} -> cursor={}",
                delegation != null ? " (delegated)" : "",
                queryId,
                shard.shardId(),
                cursorId
            );
            return new Opened(LEAF_MODE_JAVA_CURSOR, cursorId);
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

    /** True when the shard's index stores its columns as Lucene doc values (lucene primary format). */
    private static boolean isDocValuesPrimary(IndexShard shard) {
        return LUCENE_FORMAT.equals(shard.indexSettings().getSettings().get(PRIMARY_DATA_FORMAT_SETTING));
    }

    @Override
    public long next(long cursor) throws Exception {
        AnalyticsSearchBackendPlugin.LeafCursor c = openCursors.get(cursor);
        if (c == null) {
            throw new IllegalStateException("leafNext for unknown/closed cursor " + cursor);
        }
        return c.next();
    }

    @Override
    public void close(long handle) {
        // JAVA_CURSOR ids are negative; native session pointers are real (positive) addresses.
        AnalyticsSearchBackendPlugin.LeafCursor cursor = openCursors.remove(handle);
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                LOGGER.warn("failed to close doc-values leaf cursor " + handle, e);
            }
            return;
        }
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

    // ── Test hooks ──

    /** Count of live JAVA_CURSOR leaves (leak assertions in tests). */
    public int openCursorCount() {
        return openCursors.size();
    }

    /** Count of live NATIVE leases (leak assertions in tests). */
    public int openReaderCount() {
        return openReaders.size();
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
