/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import java.util.List;

/**
 * Bidirectional Arrow ↔ Calcite type converter for single types.
 *
 * <p>Used by the QTF (late-materialization) Phase C in
 * {@code LateMaterializationStageExecution} to translate the above-anchor physical fields'
 * Calcite {@link RelDataType}s into Arrow {@link ArrowType}s for the fetch-stage output
 * schema. The {@code AggregateFunction.ArrowToCalciteTypeMapper} (in the SPI module) handles
 * the inverse direction for {@code IntermediateField} resolution; this class is kept as the
 * single authority for the Calcite→Arrow direction needed outside that resolver.
 */
public final class ArrowCalciteTypes {

    private ArrowCalciteTypes() {}

    /**
     * Convert a Calcite {@link RelDataType} to the corresponding Arrow {@link Field} named
     * {@code name}.
     *
     * <p>Preferred over {@link #toArrow} for building a schema, because the two composite shapes
     * carry their members in the Field's children rather than in its ArrowType: a ROW — what an
     * OpenSearch {@code object} becomes — and an ARRAY — what a multi-valued field becomes. Either
     * one built without children describes an empty composite, which the receiving side cannot
     * populate.
     */
    public static Field toArrowField(String name, RelDataType t) {
        if (t.getSqlTypeName() == SqlTypeName.ROW) {
            List<Field> children = new ArrayList<>(t.getFieldCount());
            for (RelDataTypeField child : t.getFieldList()) {
                children.add(toArrowField(child.getName(), child.getType()));
            }
            return new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE), children);
        }
        if (t.getSqlTypeName() == SqlTypeName.ARRAY) {
            RelDataType component = t.getComponentType();
            if (component == null) {
                throw new IllegalArgumentException("ARRAY type has no component: " + t);
            }
            return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), List.of(toArrowField("element", component)));
        }
        return Field.nullable(name, toArrow(t));
    }

    /**
     * Convert a Calcite {@link RelDataType} to the corresponding Arrow type.
     *
     * <p>A ROW yields a bare struct type with no members; use {@link #toArrowField} when the
     * sub-fields matter, which is whenever a schema is being built.
     */
    public static ArrowType toArrow(RelDataType t) {
        return switch (t.getSqlTypeName()) {
            case ROW -> ArrowType.Struct.INSTANCE;
            case BIGINT -> new ArrowType.Int(64, true);
            case INTEGER -> new ArrowType.Int(32, true);
            // Match the wire Arrow type the data node emits: ShortParquetField -> Int(16),
            // ByteParquetField -> Int(8). Keeps the Stitcher's copyFromSafe types aligned.
            case SMALLINT -> new ArrowType.Int(16, true);
            case TINYINT -> new ArrowType.Int(8, true);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case REAL, FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            // Utf8View matches what the DataFusion/parquet path on the data node emits for
            // string columns; switching here keeps the coordinator-side Stitcher's pre-allocated
            // output type aligned so copyFromSafe doesn't trip on a VARCHAR/VIEWVARCHAR mismatch.
            case VARCHAR, CHAR -> ArrowType.Utf8View.INSTANCE;
            case VARBINARY, BINARY -> ArrowType.Binary.INSTANCE;
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            // TODO: TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE, TIME, DECIMAL still missing.
            // precision 9 ⇒ date_nanos; else date — must match the wire unit shards emit (Stitcher copyFromSafe).
            case TIMESTAMP -> new ArrowType.Timestamp(t.getPrecision() == 9 ? TimeUnit.NANOSECOND : TimeUnit.MILLISECOND, null);
            case ARRAY -> ArrowType.List.INSTANCE;
            default -> throw new IllegalArgumentException("Unsupported Calcite type: " + t.getSqlTypeName());
        };
    }
}
