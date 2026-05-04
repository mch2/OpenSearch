/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Tests that {@link FragmentExecutionResponse} coerces java.time cell values to
 * OpenSearch SQL canonical MySQL-style strings (e.g. "2026-01-15 10:30:00")
 * rather than {@link Object#toString()} ISO format (e.g. "2026-01-15T10:30").
 */
public class FragmentExecutionResponseTests extends OpenSearchTestCase {

    public void testLocalDateTimeCoercedToMySqlFormat() throws Exception {
        LocalDateTime ldt = LocalDateTime.of(2026, 1, 15, 10, 30, 0);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("ts"), List.<Object[]>of(row(ldt)));

        FragmentExecutionResponse roundTripped = roundTrip(response);

        assertEquals(1, roundTripped.getRows().size());
        assertEquals("2026-01-15 10:30:00", roundTripped.getRows().get(0)[0]);
    }

    public void testLocalDateTimeElidesZeroSecondsWhenFormattedAsIso() throws Exception {
        // LocalDateTime.toString() produces "2026-01-15T10:30" (no :00) when seconds are zero.
        // Ensure we don't fall back to that.
        LocalDateTime ldt = LocalDateTime.of(2026, 1, 15, 10, 30);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("ts"), List.<Object[]>of(row(ldt)));

        FragmentExecutionResponse roundTripped = roundTrip(response);
        String cell = (String) roundTripped.getRows().get(0)[0];
        assertFalse("must not contain ISO T separator: " + cell, cell.contains("T"));
        assertEquals("2026-01-15 10:30:00", cell);
    }

    public void testLocalDateCoercedToIsoDate() throws Exception {
        LocalDate date = LocalDate.of(2026, 1, 15);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("d"), List.<Object[]>of(row(date)));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        assertEquals("2026-01-15", roundTripped.getRows().get(0)[0]);
    }

    public void testLocalTimeCoercedToTimeFormat() throws Exception {
        LocalTime time = LocalTime.of(10, 30, 0);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("t"), List.<Object[]>of(row(time)));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        assertEquals("10:30:00", roundTripped.getRows().get(0)[0]);
    }

    public void testInstantCoercedToMySqlFormat() throws Exception {
        Instant instant = LocalDateTime.of(2026, 1, 15, 10, 30, 0).toInstant(ZoneOffset.UTC);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("ts"), List.<Object[]>of(row(instant)));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        assertEquals("2026-01-15 10:30:00", roundTripped.getRows().get(0)[0]);
    }

    public void testNumberAndStringPassthrough() throws Exception {
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("n", "s"), List.<Object[]>of(row(42L, "hello")));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        assertEquals(42L, roundTripped.getRows().get(0)[0]);
        assertEquals("hello", roundTripped.getRows().get(0)[1]);
    }

    public void testNullCellPreserved() throws Exception {
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("x"), List.<Object[]>of(row((Object) null)));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        assertNull(roundTripped.getRows().get(0)[0]);
    }

    public void testListOfLocalDateTimeCoerced() throws Exception {
        LocalDateTime ldt = LocalDateTime.of(2026, 1, 15, 10, 30, 0);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("arr"), List.<Object[]>of(row(List.of(ldt))));
        FragmentExecutionResponse roundTripped = roundTrip(response);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) roundTripped.getRows().get(0)[0];
        assertEquals(1, list.size());
        assertEquals("2026-01-15 10:30:00", list.get(0));
    }

    private static Object[] row(Object... cells) {
        return cells;
    }

    private static FragmentExecutionResponse roundTrip(FragmentExecutionResponse in) throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        in.writeTo(out);
        try (StreamInput si = out.bytes().streamInput()) {
            return new FragmentExecutionResponse(si);
        }
    }
}
