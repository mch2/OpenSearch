/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation.filter;

import java.util.Objects;

/**
 * Immutable value type identifying a segment (Lucene leaf / Parquet row group)
 * within a shard for delegation requests.
 *
 * @opensearch.internal
 */
public final class SegmentContext {

    private final int segmentOrdinal;
    private final int minDocId;
    private final int maxDocId;
    private final String segmentIdentifier;

    /**
     * @param segmentOrdinal    0-based ordinal mapping to Lucene LeafReaderContext / Parquet row group
     * @param minDocId          inclusive minimum doc ID in this segment
     * @param maxDocId          exclusive maximum doc ID in this segment
     * @param segmentIdentifier opaque identifier for debugging
     */
    public SegmentContext(int segmentOrdinal, int minDocId, int maxDocId, String segmentIdentifier) {
        if (segmentOrdinal < 0) {
            throw new IllegalArgumentException("segmentOrdinal must be non-negative, got " + segmentOrdinal);
        }
        if (maxDocId < minDocId) {
            throw new IllegalArgumentException(
                "maxDocId [" + maxDocId + "] must be >= minDocId [" + minDocId + "]");
        }
        this.segmentOrdinal = segmentOrdinal;
        this.minDocId = minDocId;
        this.maxDocId = maxDocId;
        this.segmentIdentifier = Objects.requireNonNull(segmentIdentifier, "segmentIdentifier");
    }

    public int getSegmentOrdinal() {
        return segmentOrdinal;
    }

    public int getMinDocId() {
        return minDocId;
    }

    public int getMaxDocId() {
        return maxDocId;
    }

    public String getSegmentIdentifier() {
        return segmentIdentifier;
    }

    @Override
    public String toString() {
        return "SegmentContext[ordinal=" + segmentOrdinal
            + ", docs=" + minDocId + ".." + maxDocId
            + ", id=" + segmentIdentifier + "]";
    }
}
