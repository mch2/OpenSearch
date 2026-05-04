/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for reading Arrow vector cells as plain Java values at the
 * external query API edge.
 */
public final class ArrowValues {

    private ArrowValues() {}

    /**
     * Returns the cell at {@code index} in {@code vector} as a Java value:
     * <ul>
     *   <li>{@code null} when the cell is null</li>
     *   <li>UTF-8 {@link String} for {@link VarCharVector} cells (rather than
     *       the raw {@code Text} that {@code getObject} returns)</li>
     *   <li>plain {@link List} for {@link ListVector} cells, recursively
     *       unwrapping each element via this method (avoids leaking Arrow's
     *       {@code JsonStringArrayList<Text>} into downstream code that
     *       only recognises standard Java types)</li>
     *   <li>{@link FieldVector#getObject} for every other vector type</li>
     * </ul>
     */
    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        if (vector instanceof ListVector listVector) {
            return listToJavaValue(listVector, index);
        }
        // Date32 (day-precision) vectors: Arrow returns the raw Integer epoch-day count
        // from getObject(). FragmentExecutionResponse's coerce() doesn't know that
        // integer represents a date, so it serializes as a raw int (e.g. "5215" instead
        // of "1984-04-12"). Convert to LocalDate here — the response's LocalDate branch
        // formats it as "yyyy-MM-dd".
        if (vector instanceof DateDayVector dateVector) {
            return LocalDate.ofEpochDay(dateVector.get(index));
        }
        // Time32(MILLI) vectors: Arrow's TimeMilliVector.getObject has a longstanding bug
        // where it treats the int32 millis-of-day as if it were epoch millis, producing a
        // LocalDateTime pinned to 1970-01-01 (e.g. "1970-01-01 09:07:00"). Downstream
        // serializers can't distinguish that from a legitimate LocalDateTime timestamp.
        // Read the raw int millis-of-day and construct a LocalTime so the response's
        // LocalTime branch formats it as "HH:mm:ss".
        if (vector instanceof TimeMilliVector timeVector) {
            int millisOfDay = timeVector.get(index);
            return LocalTime.ofNanoOfDay((long) millisOfDay * 1_000_000L);
        }
        // Time32(SECOND) vectors: TimeSecVector.getObject returns a raw Integer seconds-of-day.
        // DataFusion's make_time(i32, i32, i32) (used by the PPL SPAN-on-TIME decomposition)
        // returns Time32(SECOND), so we see this vector type on the response path.
        // Coerce to LocalTime for canonical "HH:mm:ss" formatting.
        if (vector instanceof TimeSecVector timeSecVector) {
            int secondsOfDay = timeSecVector.get(index);
            return LocalTime.ofSecondOfDay(secondsOfDay);
        }
        // Time64(MICRO/NANO) vectors: returned by DF for higher-precision time operations.
        // Arrow's getObject returns Long microseconds/nanoseconds of day. Coerce to LocalTime.
        if (vector instanceof TimeMicroVector timeMicroVector) {
            long microsOfDay = timeMicroVector.get(index);
            return LocalTime.ofNanoOfDay(microsOfDay * 1_000L);
        }
        if (vector instanceof TimeNanoVector timeNanoVector) {
            long nanosOfDay = timeNanoVector.get(index);
            return LocalTime.ofNanoOfDay(nanosOfDay);
        }
        Object obj = vector.getObject(index);
        // Catch-all for any Arrow vector whose getObject() returns Arrow's Text wrapper
        // (e.g. ViewVarCharVector / Utf8View, LargeVarCharVector). Downstream serializers
        // — FragmentExecutionResponse via writeGenericValue, in particular — only know
        // standard Java types; letting Text leak through crashes the transport channel.
        if (obj instanceof org.apache.arrow.vector.util.Text) {
            return obj.toString();
        }
        return obj;
    }

    private static List<Object> listToJavaValue(ListVector listVector, int index) {
        int start = listVector.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
        int end = listVector.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
        FieldVector inner = listVector.getDataVector();
        List<Object> result = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            result.add(toJavaValue(inner, i));
        }
        return result;
    }
}
