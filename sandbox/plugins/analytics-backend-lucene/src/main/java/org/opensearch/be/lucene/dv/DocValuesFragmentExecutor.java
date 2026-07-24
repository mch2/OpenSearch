/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.List;

/**
 * The doc-values leaf executor (spec J1): Lucene executes the scan (delegated filters select doc
 * IDs), Java bulk-decodes doc values into Arrow batches through the {@link ColumnBatchSource} seam,
 * and the native side PULLS each batch via {@link AnalyticsSearchBackendPlugin.LeafCursor#next()}
 * (leaf mode JAVA_CURSOR — DataFusion is pull-based, so the scan advances exactly one batch per
 * downcall and backpressure is inherent: a slow consumer simply stops pulling).
 *
 * <p>PoC skeleton runs segments SEQUENTIALLY on the pulling thread. Segment parallelism (a bounded
 * producer pool feeding a bounded queue) is a planned follow-on; the cursor contract doesn't change.
 *
 * <p>Counters: {@code docsRead} accumulates each visited segment's maxDoc (the universe the query
 * examined — both are recorded because their ratio is the observable filter selectivity, following
 * the opensearch-olap reader); {@code docsMatched} counts scorer-emitted docs. Per-column
 * bulk-vs-fallback decode counters live on the {@link ColumnBatchSource}.
 *
 * <p>Deleted docs: {@code Weight#scorer} does NOT filter deletions (IndexSearcher's collect loop
 * does that), so the doc-batch fill checks the segment's liveDocs explicitly. Batch order across
 * segments is docid order within this PoC (sequential scan); once segment parallelism lands the
 * cross-segment order is unspecified — every shape this PoC targets is order-insensitive above the
 * leaf.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DocValuesFragmentExecutor implements AnalyticsSearchBackendPlugin.LeafCursor {

    private static final Logger LOGGER = LogManager.getLogger(DocValuesFragmentExecutor.class);

    private final BufferAllocator allocator;
    private final IndexSearcher searcher;
    private final Weight weight;
    private final List<LeafReaderContext> leaves;
    private final ColumnBatchSource batchSource;
    private final Schema projectedSchema;
    private final int batchSize;
    private final Runnable onClose;

    // Scan cursor state (single-threaded: the native side pulls one batch at a time).
    private int leafOrd = 0;
    private DocIdSetIterator currentIterator;
    private LeafReaderContext currentLeaf;
    /** Current segment's liveDocs (null = no deletes). Weight.scorer does NOT apply deletions —
     *  that filtering lives in IndexSearcher's collect loop — so the scan must check explicitly. */
    private org.apache.lucene.util.Bits currentLiveDocs;
    private final int[] docBuffer;

    // The previous batch's export, released on the following next()/close(). The native importer
    // moves the batch contents out of the struct during the SAME leaf_next downcall, but the struct
    // wrapper itself must outlive that import — hence deferred close (mirrors the reduce-sink rule
    // that ArrowArray.close only frees the wrapper once Rust nulled the release callback).
    private ArrowArray pendingExport;
    private VectorSchemaRoot pendingRoot;

    private boolean exhausted;
    private boolean closed;

    // Fragment counters (spec: non-optional).
    private long docsRead;
    private long docsMatched;
    private long batchesEmitted;
    private long bytesEmitted;

    /**
     * @param query the Lucene query selecting doc IDs (delegated predicates, or MatchAll for a full
     *              scan); rewritten + weighted here with {@link ScoreMode#COMPLETE_NO_SCORES}
     * @param onClose releases the reader lease (runs exactly once, from {@link #close()})
     */
    public DocValuesFragmentExecutor(
        BufferAllocator allocator,
        IndexSearcher searcher,
        Query query,
        Schema projectedSchema,
        ColumnBatchSource batchSource,
        int batchSize,
        Runnable onClose
    ) throws IOException {
        this.allocator = allocator;
        this.searcher = searcher;
        this.projectedSchema = projectedSchema;
        this.batchSource = batchSource;
        this.batchSize = batchSize;
        this.onClose = onClose;
        this.leaves = searcher.getIndexReader().leaves();
        Query rewritten = searcher.rewrite(query);
        this.weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        this.docBuffer = new int[batchSize];
    }

    @Override
    public long next() throws Exception {
        releasePending();
        if (closed || exhausted) {
            return 0L;
        }
        int count = fillDocBatch();
        if (count == 0) {
            exhausted = true;
            logCompletion();
            return 0L;
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(projectedSchema, allocator);
        boolean exported = false;
        try {
            root.allocateNew();
            batchSource.decodeBatch(currentLeaf, docBuffer, count, root);
            root.setRowCount(count);
            docsMatched += count;
            batchesEmitted++;
            for (org.apache.arrow.vector.FieldVector v : root.getFieldVectors()) {
                bytesEmitted += v.getBufferSize();
            }
            ArrowArray array = ArrowArray.allocateNew(allocator);
            try {
                // Ownership of the buffers transfers into the C struct; the root is closed right
                // after (the export holds references). The native importer moves the contents out
                // during this same downcall; releasePending() frees the wrapper on the next pull.
                Data.exportVectorSchemaRoot(allocator, root, null, array);
                pendingExport = array;
                exported = true;
                return array.memoryAddress();
            } finally {
                if (exported == false) {
                    array.release();
                    array.close();
                }
            }
        } finally {
            root.close();
        }
    }

    /**
     * Advance the scan to the next non-empty doc batch. A batch never spans segments by
     * construction — the iterator is per-segment and we return at the segment boundary.
     */
    private int fillDocBatch() throws IOException {
        while (true) {
            if (currentIterator == null) {
                if (leafOrd >= leaves.size()) {
                    return 0;
                }
                LeafReaderContext leaf = leaves.get(leafOrd++);
                docsRead += leaf.reader().maxDoc();
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue; // no matches in this segment (or empty segment)
                }
                currentLeaf = leaf;
                currentLiveDocs = leaf.reader().getLiveDocs();
                currentIterator = scorer.iterator();
            }
            int count = 0;
            int doc = currentIterator.nextDoc();
            while (doc != DocIdSetIterator.NO_MORE_DOCS && count < batchSize) {
                if (currentLiveDocs == null || currentLiveDocs.get(doc)) {
                    docBuffer[count++] = doc;
                    if (count == batchSize) {
                        break;
                    }
                }
                doc = currentIterator.nextDoc();
            }
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                currentIterator = null; // segment drained; emit whatever we collected
            }
            if (count > 0) {
                return count;
            }
        }
    }

    private void releasePending() {
        if (pendingExport != null) {
            pendingExport.close();
            pendingExport = null;
        }
        if (pendingRoot != null) {
            pendingRoot.close();
            pendingRoot = null;
        }
    }

    private void logCompletion() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "[dv-leaf] scan complete: docsRead={} docsMatched={} batches={} bytes={} decode={}",
                docsRead,
                docsMatched,
                batchesEmitted,
                bytesEmitted,
                batchSource.decodeStats()
            );
        }
    }

    // ── Counters (exposed for the stats surface / tests) ──

    public long docsRead() {
        return docsRead;
    }

    public long docsMatched() {
        return docsMatched;
    }

    public long batchesEmitted() {
        return batchesEmitted;
    }

    public long bytesEmitted() {
        return bytesEmitted;
    }

    public List<ColumnBatchSource.ColumnDecodeStats> decodeStats() {
        return batchSource.decodeStats();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        releasePending();
        try {
            batchSource.close();
        } finally {
            if (onClose != null) {
                onClose.run();
            }
        }
    }
}
