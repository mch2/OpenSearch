/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.MatchAllQueryBuilder;

import java.io.IOException;

/**
 * Minimal {@link FragmentConvertor} for the Lucene backend.
 *
 * <p>MVP: serializes a {@link MatchAllQueryBuilder} as the fragment bytes for all
 * fragment types. The Lucene backend on the data node deserializes these bytes
 * back into a QueryBuilder and uses it to drive shard-level execution.
 *
 * <p>Future: walk the stripped RelNode to extract filter predicates, projections,
 * and sort orders, and produce a more specific QueryBuilder + field list.
 *
 * @opensearch.internal
 */
public class LuceneFragmentConvertor implements FragmentConvertor {

    @Override
    public byte[] convertScanFragment(String tableName, RelNode fragment) {
        return serializeMatchAll();
    }

    @Override
    public byte[] convertShuffleReadFragment(String tableName, RelNode fragment) {
        return serializeMatchAll();
    }

    private static byte[] serializeMatchAll() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(new MatchAllQueryBuilder());
            return BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize MatchAllQueryBuilder", e);
        }
    }
}
