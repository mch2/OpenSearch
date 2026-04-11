/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Tests for {@link SimpleExchangeSink}.
 */
public class SimpleExchangeSinkTests extends OpenSearchTestCase {

    private static List<Object[]> rowsOf(Object[]... rows) {
        List<Object[]> list = new ArrayList<>();
        for (Object[] row : rows) {
            list.add(row);
        }
        return list;
    }

    public void testFeedSingleResponse() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        List<String> fieldNames = List.of("name", "age");
        List<Object[]> rows = rowsOf(new Object[] { "alice", 30 }, new Object[] { "bob", 25 });
        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);

        sink.feed(response);

        assertEquals(2, sink.getRowCount());
        Iterator<Object[]> it = sink.readResult().iterator();
        assertTrue(it.hasNext());
        assertArrayEquals(new Object[] { "alice", 30 }, it.next());
        assertTrue(it.hasNext());
        assertArrayEquals(new Object[] { "bob", 25 }, it.next());
        assertTrue(it.hasNext() == false);
    }

    public void testFeedMultipleResponsesPreservesOrder() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        List<String> fields = List.of("id");

        FragmentExecutionResponse r1 = new FragmentExecutionResponse(fields, rowsOf(new Object[] { 1 }));
        FragmentExecutionResponse r2 = new FragmentExecutionResponse(fields, rowsOf(new Object[] { 2 }));
        FragmentExecutionResponse r3 = new FragmentExecutionResponse(fields, rowsOf(new Object[] { 3 }));

        sink.feed(r1);
        sink.feed(r2);
        sink.feed(r3);

        assertEquals(3, sink.getRowCount());
        Iterator<Object[]> it = sink.readResult().iterator();
        assertArrayEquals(new Object[] { 1 }, it.next());
        assertArrayEquals(new Object[] { 2 }, it.next());
        assertArrayEquals(new Object[] { 3 }, it.next());
        assertTrue(it.hasNext() == false);
    }

    public void testFieldNamesCapturedFromFirstNonEmptyResponse() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        // First response has empty field names
        FragmentExecutionResponse emptyFields = new FragmentExecutionResponse(Collections.emptyList(), rowsOf(new Object[] { "x" }));
        sink.feed(emptyFields);

        // Second response has field names
        FragmentExecutionResponse withFields = new FragmentExecutionResponse(List.of("col_a"), rowsOf(new Object[] { "y" }));
        sink.feed(withFields);

        // Field names should come from the second response
        assertEquals("y", sink.getValueAt("col_a", 1));
    }

    public void testGetValueAtValidColumnAndRow() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        List<String> fieldNames = List.of("col1", "col2");
        List<Object[]> rows = rowsOf(new Object[] { "hello", 42L });
        sink.feed(new FragmentExecutionResponse(fieldNames, rows));

        assertEquals("hello", sink.getValueAt("col1", 0));
        assertEquals(42L, sink.getValueAt("col2", 0));
    }

    public void testGetValueAtUnknownColumnReturnsNull() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        List<String> fieldNames = List.of("col1");
        List<Object[]> rows = rowsOf(new Object[] { "data" });
        sink.feed(new FragmentExecutionResponse(fieldNames, rows));

        assertNull(sink.getValueAt("nonexistent", 0));
    }

    public void testGetValueAtOutOfRangeRowReturnsNull() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        List<String> fieldNames = List.of("col1");
        List<Object[]> rows = rowsOf(new Object[] { "only_row" });
        sink.feed(new FragmentExecutionResponse(fieldNames, rows));

        assertNull(sink.getValueAt("col1", 5));
    }

    public void testEmptySinkReturnsEmptyResult() {
        SimpleExchangeSink sink = new SimpleExchangeSink();

        assertEquals(0, sink.getRowCount());
        Iterator<Object[]> it = sink.readResult().iterator();
        assertTrue(it.hasNext() == false);
    }
}
