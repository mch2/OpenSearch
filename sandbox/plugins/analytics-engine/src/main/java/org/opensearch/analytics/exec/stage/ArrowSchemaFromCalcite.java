/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a Calcite {@link RelDataType} (row type) to an Arrow {@link Schema}.
 * Used to derive the target schema for {@code RowBatchToArrowConverter} from the
 * child stage's resolved fragment row type.
 *
 * <p>All fields are nullable for MVP.
 */
final class ArrowSchemaFromCalcite {

    private ArrowSchemaFromCalcite() {}

    /**
     * Convert a Calcite row type to an Arrow schema. All fields are nullable.
     *
     * @param rowType the Calcite row type from a RelNode fragment
     * @return the corresponding Arrow schema
     */
    public static Schema arrowSchemaFromRowType(RelDataType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RelDataTypeField f : rowType.getFieldList()) {
            fields.add(buildField(f.getName(), f.getType()));
        }
        return new Schema(fields);
    }

    static Field fieldFromCalcite(RelDataTypeField f) {
        return buildField(f.getName(), f.getType());
    }

    private static Field buildField(String name, RelDataType relType) {
        SqlTypeName sqlTypeName = relType.getSqlTypeName();
        if (sqlTypeName == SqlTypeName.ARRAY) {
            // Arrow ListType fields carry a single child field describing the element type.
            // Recurse on the Calcite component type so nested arrays work too.
            Field child = buildField("item", relType.getComponentType());
            return new Field(name, new FieldType(true, new ArrowType.List(), null), List.of(child));
        }
        ArrowType arrowType = toArrowType(relType);
        return new Field(name, new FieldType(true, arrowType, null), null);
    }

    /**
     * Maps a Calcite {@link RelDataType} to an Arrow {@link ArrowType}. Takes the full
     * RelDataType (not just the SqlTypeName) so precision/scale-bearing types like
     * {@code DECIMAL(10,2)} can be carried through without losing their parameters.
     *
     * <p>Temporal conventions mirror {@code DateParquetField} / {@code DateNanosParquetField}
     * so the mid-stage input schema agrees with the Arrow vectors the data-node reader
     * emits for parquet-backed date/date_nanos columns.
     */
    private static ArrowType toArrowType(RelDataType relType) {
        SqlTypeName sqlTypeName = relType.getSqlTypeName();
        switch (sqlTypeName) {
            case BIGINT:
                return new ArrowType.Int(64, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case TINYINT:
                return new ArrowType.Int(8, true);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case FLOAT:
            case REAL:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case VARCHAR:
            case CHAR:
                return ArrowType.Utf8.INSTANCE;
            case VARBINARY:
            case BINARY:
                return ArrowType.Binary.INSTANCE;
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                // Time32(MILLI) — matches DateParquetField's TIME_ONLY branch.
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Timestamp(MILLI) — matches DateParquetField's DATETIME branch. The
                // SQL plugin emits Calcite TIMESTAMP for both `date` and `date_nanos`
                // columns; nanosecond resolution isn't carried in the Calcite type and
                // we don't currently have a signal here to pick NANOSECOND, so stay at
                // MILLISECOND for the stage-input schema.
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case DECIMAL:
                return new ArrowType.Decimal(relType.getPrecision(), relType.getScale(), 128);
            default:
                throw new IllegalArgumentException("Unsupported Calcite SQL type: " + sqlTypeName);
        }
    }
}
