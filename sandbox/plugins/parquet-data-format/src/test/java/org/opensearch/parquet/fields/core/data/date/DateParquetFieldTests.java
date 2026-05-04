/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.date;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class DateParquetFieldTests extends OpenSearchTestCase {

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

    public void testDateFieldArrowType() {
        DateParquetField field = new DateParquetField();
        ArrowType.Timestamp type = (ArrowType.Timestamp) field.getArrowType();
        assertEquals(TimeUnit.MILLISECOND, type.getUnit());
        assertNull(type.getTimezone());
        assertTrue(field.getFieldType().isNullable());
    }

    public void testDateFieldAddToGroup() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = new DateFieldMapper.DateFieldType("val");
        ManagedVSR vsr = createVSR("date-test", field, "val");
        long millis = 1700000000000L;
        field.createField(ft, vsr, millis);
        vsr.setRowCount(1);
        assertEquals(millis, ((TimeStampMilliVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testDateNanosFieldArrowType() {
        DateNanosParquetField field = new DateNanosParquetField();
        ArrowType.Timestamp type = (ArrowType.Timestamp) field.getArrowType();
        assertEquals(TimeUnit.NANOSECOND, type.getUnit());
        assertNull(type.getTimezone());
    }

    public void testDateNanosFieldAddToGroup() {
        DateNanosParquetField field = new DateNanosParquetField();
        MappedFieldType ft = new DateFieldMapper.DateFieldType("val");
        ManagedVSR vsr = createVSR("datenanos-test", field, "val");
        long nanos = 1700000000000000000L;
        field.createField(ft, vsr, nanos);
        vsr.setRowCount(1);
        assertEquals(nanos, ((TimeStampNanoVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    // --- per-format narrowing (Layer 2a) ---
    //
    // When the DateFieldType's formatter pattern is a date-only bucket (e.g.
    // "basic_date", "year_month_day"), the Arrow column narrows to Date32 so the
    // parquet runtime schema agrees with OpenSearchSchemaBuilder's DATE declaration.
    // Time-only patterns narrow to Time(MILLISECOND); combined/full-datetime/epoch
    // stays Timestamp(MILLI).

    public void testDateFieldArrowTypeNarrowsToDate32ForDateOnlyFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "basic_date");
        ArrowType type = field.getArrowType(ft);
        assertTrue("basic_date must narrow to Arrow Date32, got " + type, type instanceof ArrowType.Date);
        assertEquals(org.apache.arrow.vector.types.DateUnit.DAY, ((ArrowType.Date) type).getUnit());
    }

    public void testDateFieldArrowTypeNarrowsToTimeForTimeOnlyFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "hour_minute_second");
        ArrowType type = field.getArrowType(ft);
        assertTrue("hour_minute_second must narrow to Arrow Time, got " + type, type instanceof ArrowType.Time);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Time) type).getUnit());
    }

    public void testDateFieldArrowTypeStaysTimestampForFullDatetimeFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "basic_date_time");
        ArrowType type = field.getArrowType(ft);
        assertTrue("full-datetime format must stay Timestamp, got " + type, type instanceof ArrowType.Timestamp);
        assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Timestamp) type).getUnit());
    }

    public void testDateFieldArrowTypeStaysTimestampForCombinedCrossBucketFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "yyyy-MM-dd||epoch_millis");
        ArrowType type = field.getArrowType(ft);
        assertTrue("combined-bucket format must stay Timestamp, got " + type, type instanceof ArrowType.Timestamp);
    }

    public void testDateFieldArrowTypeStaysTimestampWithoutMappedFieldType() {
        DateParquetField field = new DateParquetField();
        // No-arg overload must preserve legacy behavior (Timestamp) for callers that
        // don't have a MappedFieldType context.
        assertTrue(field.getArrowType() instanceof ArrowType.Timestamp);
    }

    public void testDateFieldAddToGroupWritesDateDayForDateOnlyFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "basic_date");
        ManagedVSR vsr = createVSR("datefmt-basic-date", field, "val", ft);
        // 2023-11-14 UTC = epoch day 19675 = 1700000000000 ms ÷ 86_400_000
        long millis = 1700000000000L;
        field.createField(ft, vsr, millis);
        vsr.setRowCount(1);
        int expectedEpochDay = (int) (millis / 86_400_000L);
        assertEquals(expectedEpochDay, ((DateDayVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testDateFieldAddToGroupWritesTimeMilliForTimeOnlyFormat() {
        DateParquetField field = new DateParquetField();
        MappedFieldType ft = dateFieldTypeWithFormat("val", "hour_minute_second");
        ManagedVSR vsr = createVSR("datefmt-hms", field, "val", ft);
        // 12:34:56 UTC = 45_296_000 ms-of-day; passed as millis-since-epoch it's
        // 1700045296000 which maps to the time component (1700045296000 % 86400000).
        long millis = 1700045296000L;
        field.createField(ft, vsr, millis);
        vsr.setRowCount(1);
        int expectedMillisOfDay = (int) (millis % 86_400_000L);
        assertEquals(expectedMillisOfDay, ((TimeMilliVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    private MappedFieldType dateFieldTypeWithFormat(String name, String format) {
        return new DateFieldMapper.DateFieldType(name, DateFormatter.forPattern(format));
    }

    private ManagedVSR createVSR(String id, ParquetField pf, String fieldName) {
        Schema schema = new Schema(List.of(new Field(fieldName, pf.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private ManagedVSR createVSR(String id, ParquetField pf, String fieldName, MappedFieldType ft) {
        Schema schema = new Schema(List.of(new Field(fieldName, pf.getFieldType(ft), null)));
        BufferAllocator child = allocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private void cleanupVSR(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}
