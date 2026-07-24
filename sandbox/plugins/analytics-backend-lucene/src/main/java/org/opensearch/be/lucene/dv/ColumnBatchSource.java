/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

import java.io.IOException;
import java.util.List;

/**
 * The decode seam of the doc-values leaf (PoC-2 contract). The fragment executor selects doc IDs
 * (Lucene scorer) and hands ascending batches here; the implementation materializes the projected
 * columns into the supplied Arrow root. PoC 1 implements this over Lucene's doc-values iterators
 * ({@link LuceneColumnBatchSource}); PoC 2 replaces the implementation with a native reader driving
 * the same files directly — the docid-selection contract, schema derivation, and stream framing all
 * stay. Outputs are Arrow-only (no Lucene types) so an FFM-backed implementation is expressible.
 *
 * <p>NOT thread-safe: doc-values iterators are forward-only per instance, so one source instance
 * serves one scan thread. Callers must feed strictly ascending doc IDs within a batch and strictly
 * ascending batches within a segment; a batch never spans segments.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ColumnBatchSource extends Releasable {

    /**
     * Decode the given ascending {@code docIds[0..count)} of segment {@code leaf} into {@code out}.
     * The root's schema is the fragment's projected output schema; the implementation fills every
     * column vector for rows {@code [0, count)} (missing values become Arrow nulls) but does NOT set
     * the row count — the caller does, after all columns are in.
     */
    void decodeBatch(LeafReaderContext leaf, int[] docIds, int count, VectorSchemaRoot out) throws IOException;

    /** Per-column decode counters accumulated since this source was created. */
    List<ColumnDecodeStats> decodeStats();

    /**
     * Per-column decode counters. {@code bulkDecodeBatches} counts batches served by a codec bulk
     * API; {@code perDocFallbackBatches} counts batches that fell back to per-doc {@code advanceExact}.
     * Without this split, "bulk decode working" and "silently on the slow path" are indistinguishable
     * and benchmark numbers are uninterpretable.
     */
    record ColumnDecodeStats(String column, long bulkDecodeBatches, long perDocFallbackBatches, long decodeNanos) {
    }
}
