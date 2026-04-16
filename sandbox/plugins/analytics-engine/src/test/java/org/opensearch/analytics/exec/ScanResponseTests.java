/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link ScanResponse} serialization and getters.
 */
public class ScanResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTripMixedTypes() throws IOException {
        List<String> fieldNames = List.of("name", "count", "score", "rank", "nullable");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "alice", 42L, 3.14, 1, null });
        rows.add(new Object[] { "bob", 100L, 2.71, 2, null });

        ScanResponse original = new ScanResponse(fieldNames, rows);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ScanResponse deserialized = new ScanResponse(in);

        assertEquals(fieldNames, deserialized.getFieldNames());
        assertEquals(rows.size(), deserialized.getRows().size());
        for (int r = 0; r < rows.size(); r++) {
            Object[] expectedRow = rows.get(r);
            Object[] actualRow = deserialized.getRows().get(r);
            assertEquals(expectedRow.length, actualRow.length);
            for (int c = 0; c < expectedRow.length; c++) {
                assertEquals(expectedRow[c], actualRow[c]);
            }
        }
    }

    public void testSerializationRoundTripEmptyRows() throws IOException {
        List<String> fieldNames = List.of("col_a", "col_b");
        List<Object[]> rows = new ArrayList<>();

        ScanResponse original = new ScanResponse(fieldNames, rows);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ScanResponse deserialized = new ScanResponse(in);

        assertEquals(fieldNames, deserialized.getFieldNames());
        assertTrue(deserialized.getRows().isEmpty());
    }

    public void testGetters() {
        List<String> fieldNames = List.of("x", "y");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "hello", 99L });

        ScanResponse response = new ScanResponse(fieldNames, rows);

        assertEquals(fieldNames, response.getFieldNames());
        assertEquals(1, response.getRows().size());
        assertArrayEquals(new Object[] { "hello", 99L }, response.getRows().get(0));
    }

    public void testReserializationIsStable() throws IOException {
        List<String> fieldNames = List.of("city", "population");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "Seattle", 750_000L });
        rows.add(new Object[] { "Portland", 650_000L });

        ScanResponse original = new ScanResponse(fieldNames, rows);

        BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        ScanResponse deserialized = new ScanResponse(out1.bytes().streamInput());

        BytesStreamOutput out2 = new BytesStreamOutput();
        deserialized.writeTo(out2);

        assertArrayEquals(
            "Re-serialized ScanResponse must be byte-for-byte identical",
            out1.bytes().toBytesRef().bytes,
            out2.bytes().toBytesRef().bytes
        );
    }
}
