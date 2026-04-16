/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link MetricsInstrumentedSink}.
 */
public class MetricsInstrumentedSinkTests extends OpenSearchTestCase {

    private static List<Object[]> makeRows(int count, int cols) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Object[] row = new Object[cols];
            for (int c = 0; c < cols; c++) {
                row[c] = randomLong();
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<String> makeFieldNames(int count) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            names.add("field_" + i);
        }
        return names;
    }

    /**
     * Feeding a FragmentExecutionResponse with row data increments both
     * rowsProcessed and bytesRead on the StageMetrics instance.
     *
     * Validates: Requirements 2.1, 2.2
     */
    public void testFeedVsrIncrementsRowsAndBytes() {
        StageMetrics metrics = new StageMetrics(0);
        SimpleExchangeSink delegate = new SimpleExchangeSink();
        MetricsInstrumentedSink sink = new MetricsInstrumentedSink(metrics, delegate);

        int fieldCount = 3;
        int rowCount = 10;
        List<String> fieldNames = makeFieldNames(fieldCount);
        List<Object[]> rows = makeRows(rowCount, fieldCount);
        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);

        sink.feed(response);

        assertEquals("rowsProcessed must equal the number of rows fed", 10L, metrics.getRowsProcessed());
        assertTrue("bytesRead must be > 0 after feeding row data", metrics.getBytesRead() > 0);
        // Approximate byte estimate: rows * fields * 8
        assertEquals("bytesRead must equal rows * fields * 8", (long) rowCount * fieldCount * 8L, metrics.getBytesRead());
    }

    /**
     * Feeding a FragmentExecutionResponse with 5 rows increments rowsProcessed to 5
     * and bytesRead to an approximate positive value.
     *
     * Validates: Requirements 2.3
     */
    public void testFeedDataResponseIncrementsRowsOnly() {
        StageMetrics metrics = new StageMetrics(0);
        SimpleExchangeSink delegate = new SimpleExchangeSink();
        MetricsInstrumentedSink sink = new MetricsInstrumentedSink(metrics, delegate);

        int fieldCount = 4;
        int rowCount = 5;
        List<String> fieldNames = makeFieldNames(fieldCount);
        List<Object[]> rows = makeRows(rowCount, fieldCount);
        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);

        sink.feed(response);

        assertEquals("rowsProcessed must equal 5 after feeding 5 rows", 5L, metrics.getRowsProcessed());
        assertTrue("bytesRead must be > 0 (approximate)", metrics.getBytesRead() > 0);
    }

    /**
     * All non-feed ExchangeSink methods delegate unchanged to the wrapped sink:
     * close, readResult, getRowCount, getValueAt.
     *
     * Validates: Requirements 2.4
     */
    public void testDelegationForCloseAndReadResult() {
        StageMetrics metrics = new StageMetrics(0);
        SimpleExchangeSink delegate = new SimpleExchangeSink();
        MetricsInstrumentedSink sink = new MetricsInstrumentedSink(metrics, delegate);

        // Feed some data through the delegate first
        List<String> fieldNames = List.of("col_a", "col_b");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "hello", 42L });
        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);
        sink.feed(response);

        // close() should delegate without error
        sink.close();

        // readResult() should return the same rows the delegate collected
        Iterable<Object[]> result = sink.readResult();
        int count = 0;
        for (Object[] row : result) {
            count++;
        }
        assertEquals("readResult must return the same rows as the delegate", 1, count);

        // getRowCount() should delegate
        assertEquals("getRowCount must delegate to the wrapped sink", 1L, sink.getRowCount());

        // getValueAt() should delegate
        assertEquals("getValueAt must delegate to the wrapped sink", "hello", sink.getValueAt("col_a", 0));
        assertEquals("getValueAt must delegate to the wrapped sink", 42L, sink.getValueAt("col_b", 0));
    }

    /**
     * Metadata-only responses (hasMetadata() == true) should not increment
     * rowsProcessed or bytesRead — they carry no row data.
     *
     * Validates: Requirements 2.3
     */
    public void testShuffleManifestPayloadSkipsCounting() {
        StageMetrics metrics = new StageMetrics(0);
        SimpleExchangeSink delegate = new SimpleExchangeSink();
        MetricsInstrumentedSink sink = new MetricsInstrumentedSink(metrics, delegate);

        // Metadata-only response — hasMetadata() returns true
        FragmentExecutionResponse metadataResponse = new FragmentExecutionResponse(Map.of("manifest", "shard-0"));

        sink.feed(metadataResponse);

        assertEquals("rowsProcessed must be 0 for metadata-only responses", 0L, metrics.getRowsProcessed());
        assertEquals("bytesRead must be 0 for metadata-only responses", 0L, metrics.getBytesRead());
    }
}
