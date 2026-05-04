/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * Tests for {@link ArrowValues#toJavaValue}.
 *
 * <p>Covers the conversions that matter at the SQL-API boundary, where downstream
 * code only recognises plain Java types ({@link String}, {@link List}, etc.) and
 * blows up on Arrow's {@code Text} / {@code JsonStringArrayList} wrappers.
 */
public class ArrowValuesTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testNullCellReturnsNull() {
        try (IntVector v = new IntVector("x", allocator)) {
            v.allocateNew(1);
            v.setNull(0);
            v.setValueCount(1);
            assertNull(ArrowValues.toJavaValue(v, 0));
        }
    }

    public void testVarCharVectorReturnsPlainString() {
        try (VarCharVector v = new VarCharVector("name", allocator)) {
            v.allocateNew(1);
            v.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            v.setValueCount(1);
            Object result = ArrowValues.toJavaValue(v, 0);
            assertEquals(String.class, result.getClass());
            assertEquals("hello", result);
        }
    }

    /**
     * Regression test for the take() rendering bug: a ListVector wrapping a
     * VarCharVector must come out as a plain {@code List<String>}, not as
     * Arrow's {@code JsonStringArrayList<Text>}. The old code returned the raw
     * {@code getObject()} value, whose elements are {@code Text} (not String);
     * downstream serialization would then either fail or stringify the list to
     * its {@code toString()} (a JSON-encoded string).
     */
    public void testListVectorOfStringReturnsListOfString() {
        try (ListVector listVector = ListVector.empty("take", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.varChar().writeVarChar("Amber JOHnny");
            writer.varChar().writeVarChar("Hattie");
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);

            assertTrue("expected List, got " + result.getClass(), result instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) result;
            assertEquals(2, list.size());
            assertEquals(String.class, list.get(0).getClass());
            assertEquals("Amber JOHnny", list.get(0));
            assertEquals("Hattie", list.get(1));
        }
    }

    /**
     * Empty list elements (e.g. {@code take(col, 0)}) must round-trip as an
     * empty Java list, not as null.
     */
    public void testListVectorEmptyReturnsEmptyList() {
        try (ListVector listVector = ListVector.empty("take", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);
            assertTrue(result instanceof List);
            assertEquals(0, ((List<?>) result).size());
        }
    }

    /**
     * Arrow's {@link DateDayVector#getObject} returns an {@link Integer} epoch-day count,
     * which leaks to {@link org.opensearch.analytics.exec.action.FragmentExecutionResponse}
     * and serializes as a raw integer ({@code 5215}) instead of an ISO date
     * ({@code "1984-04-12"}). Coerce to {@link LocalDate} so the response's existing
     * {@code LocalDate → "yyyy-MM-dd"} branch fires.
     *
     * <p>Day 5215 = 1984-04-12 (reference date for the {@code date-formats} test index).
     * Regression for {@code testCountByDateTypeSpanWithDifferentUnits} and siblings.
     */
    public void testDateDayVectorReturnsLocalDate() {
        try (DateDayVector v = new DateDayVector("d", allocator)) {
            v.allocateNew(1);
            v.set(0, 5215);
            v.setValueCount(1);
            Object result = ArrowValues.toJavaValue(v, 0);
            assertEquals("DateDayVector must coerce to LocalDate, not raw Integer epoch-day", LocalDate.class, result.getClass());
            assertEquals(LocalDate.of(1984, 4, 12), result);
        }
    }

    /**
     * Arrow's {@link TimeMilliVector#getObject} has a known bug: it treats the int32
     * millis-of-day as if it were epoch millis, producing a {@link java.time.LocalDateTime}
     * pinned to 1970-01-01 (e.g. {@code "1970-01-01 09:07:00"}). The downstream response
     * formatter can't distinguish this from a legitimate {@code LocalDateTime} timestamp.
     *
     * <p>Coerce to {@link LocalTime} here (where we still have the Arrow vector type) so
     * the response's existing {@code LocalTime → "HH:mm:ss"} branch fires and the user
     * sees {@code "09:07:00"}.
     *
     * <p>Millis-of-day {@code 9*3600_000 + 7*60_000 + 42_000 = 32_862_000} = 09:07:42.
     * Regression for {@code testCountByTimeTypeSpanWithDifferentUnits} and
     * {@code testCountByNullableTimeSpan}.
     */
    public void testTimeMilliVectorReturnsLocalTime() {
        try (TimeMilliVector v = new TimeMilliVector("t", allocator)) {
            v.allocateNew(1);
            v.set(0, (9 * 3600 + 7 * 60 + 42) * 1000);
            v.setValueCount(1);
            Object result = ArrowValues.toJavaValue(v, 0);
            assertEquals("TimeMilliVector must coerce to LocalTime, not Arrow's buggy LocalDateTime", LocalTime.class, result.getClass());
            assertEquals(LocalTime.of(9, 7, 42), result);
        }
    }

    /**
     * Lists of integers should also unwrap recursively — the inner Int vector
     * already returns boxed Integers from getObject, so this is a sanity check
     * that the recursion handles non-VarChar inner types correctly.
     */
    public void testListVectorOfIntReturnsListOfInteger() {
        try (ListVector listVector = ListVector.empty("ids", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Int(32, true)));
            UnionListWriter writer = listVector.getWriter();
            writer.startList();
            writer.integer().writeInt(7);
            writer.integer().writeInt(11);
            writer.endList();
            writer.setValueCount(1);

            Object result = ArrowValues.toJavaValue(listVector, 0);
            assertTrue(result instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) result;
            assertEquals(List.of(7, 11), list);
        }
    }
}
