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
 * Immutable response from a delegated filter operation.
 * Contains a bitset of matching doc IDs in {@code BitSet.toLongArray()} format,
 * relative to the request's {@link SegmentContext#getMinDocId()}.
 *
 * @opensearch.internal
 */
public final class FilterDelegationResponse {

    private final long[] matchingDocIds;
    private final int docCount;

    /**
     * @param matchingDocIds bitset in {@code BitSet.toLongArray()} format
     * @param docCount       number of matching documents
     */
    public FilterDelegationResponse(long[] matchingDocIds, int docCount) {
        Objects.requireNonNull(matchingDocIds, "matchingDocIds");
        if (docCount < 0) {
            throw new IllegalArgumentException("docCount must be non-negative, got " + docCount);
        }
        this.matchingDocIds = matchingDocIds.clone();
        this.docCount = docCount;
    }

    public long[] getMatchingDocIds() {
        return matchingDocIds.clone();
    }

    public int getDocCount() {
        return docCount;
    }

    @Override
    public String toString() {
        return "FilterDelegationResponse[docCount=" + docCount
            + ", bitsetWords=" + matchingDocIds.length + "]";
    }
}
