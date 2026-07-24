/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Typed failure for a doc-values leaf fragment open/decode that hit a v1 exclusion (multi-valued
 * field, text without doc values, unsupported mapping type, …). Carrying a structured reason lets
 * the router distinguish "this query shape is out of the DV leaf's v1 scope — fall back" from a
 * genuine execution failure. Never a silent wrong answer: the fragment fails before emitting rows.
 *
 * @opensearch.internal
 */
public class DocValuesLeafUnsupportedException extends RuntimeException {

    /** What made the fragment ineligible for the doc-values leaf. */
    public enum Reason {
        /** Mapping type outside the v1 table (ip, geo_*, binary, nested, half_float, …). */
        UNSUPPORTED_FIELD_TYPE,
        /** text (or other type with no doc values), or doc_values disabled in the mapping. */
        NO_DOC_VALUES,
        /** Field carries more than one value per doc (SORTED_SET / SORTED_NUMERIC cardinality > 1). */
        MULTI_VALUED,
        /** Projected column not present in the index mapping. */
        UNKNOWN_FIELD,
        /** Delegated filter shape the DV leaf cannot evaluate (e.g. INTERLEAVED boolean tree). */
        UNSUPPORTED_DELEGATION,
        /** The shard's primary data format has no Lucene reader to decode from. */
        NO_LUCENE_READER
    }

    private final Reason reason;

    public DocValuesLeafUnsupportedException(Reason reason, String message) {
        super("[" + reason + "] " + message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
