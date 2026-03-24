/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.delegation.filter;

import org.opensearch.analytics.delegation.DelegationTarget;
import org.opensearch.analytics.delegation.DelegationType;

/**
 * Interface for delegation contexts that can handle filter delegation.
 * Implemented by backends that evaluate filter predicates on behalf of
 * another backend (e.g., Lucene evaluating indexed field predicates
 * while DataFusion scans Parquet).
 *
 * @opensearch.internal
 */
public interface FilterDelegationTarget extends DelegationTarget {

    /**
     * Evaluates a filter predicate for a segment doc range and returns
     * matching doc IDs as a bitset.
     *
     * @param targetBackend  the backend name handling this delegation
     * @param segmentOrd     0-based segment ordinal
     * @param minDocId       inclusive minimum doc ID
     * @param maxDocId       exclusive maximum doc ID
     * @return matching doc IDs in {@code BitSet.toLongArray()} format
     */
    long[] delegateFilter(String targetBackend,
                          int segmentOrd, int minDocId, int maxDocId);

    /**
     * Returns segment max docs for IndexedTableProvider setup.
     * Each entry is the maxDoc for one segment (from DirectoryReader leaves).
     *
     * @return segment max docs array, or null if not applicable
     */
    default long[] getSegmentMaxDocs() {
        return null;
    }

    @Override
    default DelegationType type() {
        return DelegationType.FILTER;
    }
}
