/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.number;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for OpenSearch {@code half_float} values.
 *
 * <p>Widened to Arrow {@code Float32} ({@link Float4Vector}) — OpenSearch declares
 * {@code half_float} as IEEE 754 binary16 on disk, but Calcite's {@code SqlTypeName}
 * vocabulary has no Float16 variant. Writing Float32 here lets the Calcite schema
 * declare {@code REAL} (see {@code OpenSearchSchemaBuilder.mapFieldType("half_float")})
 * without a substrait/parquet type-mismatch ("different type (Utf8) than ... (Float16)")
 * at read time.
 *
 * <p>Trade-off: on-disk parquet values are 32 bits per half_float instead of 16.
 * Preserving Float16 would require a substrait "Cast-above-scan" rule that widens
 * Float16 → Float32 at read time; that path is tracked in
 * {@code tasks/fixes/deferred-half-scaled-float.md}.
 */
public class HalfFloatParquetField extends ParquetField {

    /** Creates a new HalfFloatParquetField. */
    public HalfFloatParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        // NumberFieldMapper.HALF_FLOAT.parse returns a Float (IEEE 754 binary32 value
        // that falls within the half_float representable range). Previously this class
        // tried to cast parseValue to Short — that matched a Float16-vector write path
        // but would ClassCastException against the mapper's actual Float output.
        float v = ((Number) parseValue).floatValue();
        ((Float4Vector) managedVSR.getVector(mappedFieldType.name())).setSafe(managedVSR.getRowCount(), v);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
