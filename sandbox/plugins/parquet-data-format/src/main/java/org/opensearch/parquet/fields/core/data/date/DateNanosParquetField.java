/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.date;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for {@code date_nanos}-typed OpenSearch mappings. Mirrors
 * {@link DateParquetField}'s per-format narrowing: date-only formats → Arrow
 * {@code Date32}, time-only formats → {@code Time(NANOSECOND)}, everything else
 * → {@code Timestamp(NANOSECOND)} (the legacy default).
 *
 * <p>See {@link DateParquetField}'s javadoc for the rationale — the narrowing
 * keeps the runtime parquet schema in agreement with the Calcite/substrait
 * schema declaration so DataFusion doesn't reject plans.
 */
public class DateNanosParquetField extends ParquetField {

    /** Creates a new DateNanosParquetField. */
    public DateNanosParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        long nanos = (long) parseValue;
        DateFormatClassifier.Bucket bucket = bucketOf(mappedFieldType);
        switch (bucket) {
            case DATE_ONLY -> {
                int epochDay = (int) Math.floorDiv(nanos, 86_400_000_000_000L);
                ((DateDayVector) managedVSR.getVector(mappedFieldType.name()))
                    .setSafe(managedVSR.getRowCount(), epochDay);
            }
            // TIME_ONLY for date_nanos would need Calcite TIME with ns precision, which
            // Calcite SqlTypeName doesn't support (maxPrecision for TIME is 3). Schema
            // builder widens this case back to TIMESTAMP, so the writer must too.
            case TIME_ONLY, DATETIME -> ((TimeStampNanoVector) managedVSR.getVector(mappedFieldType.name()))
                .setSafe(managedVSR.getRowCount(), nanos);
        }
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public ArrowType getArrowType(MappedFieldType mappedFieldType) {
        return switch (bucketOf(mappedFieldType)) {
            case DATE_ONLY -> new ArrowType.Date(DateUnit.DAY);
            // See addToGroup: Calcite can't represent Time64(NANO), so date_nanos
            // time-only falls through to the legacy Timestamp(NANO) path.
            case TIME_ONLY, DATETIME -> new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        };
    }

    @Override
    public FieldType getFieldType(MappedFieldType mappedFieldType) {
        return FieldType.nullable(getArrowType(mappedFieldType));
    }

    private static DateFormatClassifier.Bucket bucketOf(MappedFieldType mappedFieldType) {
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType dft
                && dft.dateTimeFormatter() != null) {
            return DateFormatClassifier.classify(dft.dateTimeFormatter().pattern());
        }
        return DateFormatClassifier.Bucket.DATETIME;
    }
}
