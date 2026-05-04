/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.date;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for {@code date}-typed OpenSearch mappings. Narrows the Arrow column
 * type to {@code Date32} or {@code Time(MILLISECOND)} when the mapping's
 * {@code format:} header classifies as date-only or time-only (per
 * {@link DateFormatClassifier}); otherwise falls back to the legacy
 * {@code Timestamp(MILLISECOND)} column to cover full-datetime, epoch, and
 * cross-bucket combined formats.
 *
 * <p>The narrowing keeps the parquet file's runtime Arrow schema in agreement with
 * the Calcite/substrait schema declaration emitted by
 * {@code analytics-engine}'s {@code OpenSearchSchemaBuilder} — otherwise DataFusion
 * rejects the substrait plan with {@code "different type (Date32) than ... (Timestamp(ms))"}
 * (see also task #14's Float64 vs Float32 fix in {@code OpenSearchSchemaBuilder}).
 *
 * <p>Legacy no-arg {@link #getArrowType()}/{@link #getFieldType()} continue to
 * return the widest type (Timestamp) so any caller without a
 * {@link MappedFieldType} context (e.g. the raw
 * {@link org.opensearch.parquet.fields.ArrowFieldRegistry#getParquetField}
 * lookup in test harnesses) is unaffected.
 */
public class DateParquetField extends ParquetField {

    /** Creates a new DateParquetField. */
    public DateParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        long millis = (long) parseValue;
        DateFormatClassifier.Bucket bucket = bucketOf(mappedFieldType);
        switch (bucket) {
            case DATE_ONLY -> {
                int epochDay = (int) Math.floorDiv(millis, 86_400_000L);
                ((DateDayVector) managedVSR.getVector(mappedFieldType.name()))
                    .setSafe(managedVSR.getRowCount(), epochDay);
            }
            case TIME_ONLY -> {
                int msOfDay = (int) Math.floorMod(millis, 86_400_000L);
                ((TimeMilliVector) managedVSR.getVector(mappedFieldType.name()))
                    .setSafe(managedVSR.getRowCount(), msOfDay);
            }
            case DATETIME -> ((TimeStampMilliVector) managedVSR.getVector(mappedFieldType.name()))
                .setSafe(managedVSR.getRowCount(), millis);
        }
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public ArrowType getArrowType(MappedFieldType mappedFieldType) {
        return switch (bucketOf(mappedFieldType)) {
            case DATE_ONLY -> new ArrowType.Date(DateUnit.DAY);
            case TIME_ONLY -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case DATETIME -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        };
    }

    @Override
    public FieldType getFieldType(MappedFieldType mappedFieldType) {
        return FieldType.nullable(getArrowType(mappedFieldType));
    }

    /** Resolves the classifier bucket from a date field's mapping. Falls back to
     *  DATETIME when the mapping isn't a {@link DateFieldMapper.DateFieldType} or
     *  carries no usable pattern — that's the widest, safest Arrow type. */
    private static DateFormatClassifier.Bucket bucketOf(MappedFieldType mappedFieldType) {
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType dft
                && dft.dateTimeFormatter() != null) {
            return DateFormatClassifier.classify(dft.dateTimeFormatter().pattern());
        }
        return DateFormatClassifier.Bucket.DATETIME;
    }
}
