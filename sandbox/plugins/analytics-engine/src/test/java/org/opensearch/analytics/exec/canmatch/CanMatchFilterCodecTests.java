/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Roundtrip tests for {@link CanMatchFilter#listToBytes} / {@link CanMatchFilter#listFromBytes}.
 * Covers empty/null edge cases, single filter, multiple filters, and extreme long values.
 */
public class CanMatchFilterCodecTests extends OpenSearchTestCase {

    public void testEmptyListEncodesToZeroBytes() throws IOException {
        assertEquals(0, CanMatchFilter.listToBytes(List.of()).length);
    }

    public void testNullListEncodesToZeroBytes() throws IOException {
        assertEquals(0, CanMatchFilter.listToBytes(null).length);
    }

    public void testZeroBytesDecodesToEmptyList() throws IOException {
        assertTrue(CanMatchFilter.listFromBytes(new byte[0]).isEmpty());
        assertTrue(CanMatchFilter.listFromBytes(null).isEmpty());
    }

    public void testSingleFilterRoundtrip() throws IOException {
        CanMatchFilter input = new CanMatchFilter("@timestamp", 1_000L, 2_000L);
        List<CanMatchFilter> out = CanMatchFilter.listFromBytes(CanMatchFilter.listToBytes(List.of(input)));
        assertEquals(1, out.size());
        assertEqualsFilter(input, out.get(0));
    }

    public void testMultipleFiltersRoundtripPreservesOrder() throws IOException {
        List<CanMatchFilter> input = List.of(
            new CanMatchFilter("a", 0, 100),
            new CanMatchFilter("b", -50, 50),
            new CanMatchFilter("c", Long.MIN_VALUE, Long.MAX_VALUE)
        );
        List<CanMatchFilter> out = CanMatchFilter.listFromBytes(CanMatchFilter.listToBytes(input));
        assertEquals(input.size(), out.size());
        for (int i = 0; i < input.size(); i++) {
            assertEqualsFilter(input.get(i), out.get(i));
        }
    }

    public void testExtremeLongValues() throws IOException {
        CanMatchFilter input = new CanMatchFilter("col", Long.MIN_VALUE, Long.MAX_VALUE);
        List<CanMatchFilter> out = CanMatchFilter.listFromBytes(CanMatchFilter.listToBytes(List.of(input)));
        assertEqualsFilter(input, out.get(0));
    }

    private static void assertEqualsFilter(CanMatchFilter expected, CanMatchFilter actual) {
        assertEquals("column", expected.getColumnName(), actual.getColumnName());
        assertEquals("min", expected.getMinValue(), actual.getMinValue());
        assertEquals("max", expected.getMaxValue(), actual.getMaxValue());
    }
}
