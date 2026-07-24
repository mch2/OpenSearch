/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests the Lucene-primary merge path of {@link RowIdRemappingDocValuesProducer}: when there is no
 * {@link org.opensearch.index.engine.dataformat.RowIdMapping} (parquet is not the authority), the
 * {@code ___row_id} field must be assigned NEW sequential global row IDs offset by the merge
 * driver's per-segment {@code rowIdOffset} — not throw {@code UnsupportedOperationException}
 * (issue #21508). Mirrors the flush path's sequential assignment.
 */
public class RowIdRemappingDocValuesProducerTests extends OpenSearchTestCase {

    public void testSequentialRowIdsStartAtOffset() throws Exception {
        int maxDoc = randomIntBetween(5, 100);
        int offset = randomIntBetween(0, 100_000);
        RowIdRemappingDocValuesProducer.SequentialRowIdDocValues dv = new RowIdRemappingDocValuesProducer.SequentialRowIdDocValues(
            maxDoc,
            offset
        );
        // Global row id for doc i is offset + i, contiguous across the segment.
        for (int i = 0; i < maxDoc; i++) {
            assertTrue(dv.advanceExact(i));
            assertEquals(i, dv.docID());
            assertEquals(1, dv.docValueCount());
            assertEquals((long) offset + i, dv.nextValue());
        }
    }

    public void testNextDocIteratesOffsetRange() throws Exception {
        int maxDoc = randomIntBetween(5, 50);
        int offset = randomIntBetween(0, 1000);
        RowIdRemappingDocValuesProducer.SequentialRowIdDocValues dv = new RowIdRemappingDocValuesProducer.SequentialRowIdDocValues(
            maxDoc,
            offset
        );
        for (int i = 0; i < maxDoc; i++) {
            assertEquals(i, dv.nextDoc());
            assertEquals((long) offset + i, dv.nextValue());
        }
        assertEquals(SortedNumericDocValues.NO_MORE_DOCS, dv.nextDoc());
    }

    /**
     * The concatenation invariant the merge relies on: segments assigned back-to-back offsets
     * (0, maxDoc0, maxDoc0+maxDoc1, ...) produce a single contiguous 0..N-1 global id space with
     * no gaps or overlaps — exactly what RowIdRemappingOneMerge.wrapForMerge accumulates.
     */
    public void testOffsetsConcatenateContiguously() throws Exception {
        int[] segMaxDocs = { randomIntBetween(1, 20), randomIntBetween(1, 20), randomIntBetween(1, 20) };
        long expected = 0;
        int offset = 0;
        for (int seg = 0; seg < segMaxDocs.length; seg++) {
            RowIdRemappingDocValuesProducer.SequentialRowIdDocValues dv = new RowIdRemappingDocValuesProducer.SequentialRowIdDocValues(
                segMaxDocs[seg],
                offset
            );
            for (int doc = 0; doc < segMaxDocs[seg]; doc++) {
                dv.advanceExact(doc);
                assertEquals("global id must be contiguous across segments", expected, dv.nextValue());
                expected++;
            }
            offset += segMaxDocs[seg]; // what wrapForMerge does: nextRowIdOffset += wrapped.maxDoc()
        }
    }
}
