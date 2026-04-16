/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link ArrowSchemaFromCalcite}.
 *
 * Validates: Requirement 5.4
 */
public class ArrowSchemaFromCalciteTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // ---- BIGINT, DOUBLE, VARCHAR → correct Arrow types ----

    public void testBigintDoubleVarchar() {
        RelDataType rowType = typeFactory.builder()
            .add("id", SqlTypeName.BIGINT)
            .add("score", SqlTypeName.DOUBLE)
            .add("name", SqlTypeName.VARCHAR)
            .build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        assertEquals(3, schema.getFields().size());

        Field idField = schema.getFields().get(0);
        assertEquals("id", idField.getName());
        assertEquals(new ArrowType.Int(64, true), idField.getType());

        Field scoreField = schema.getFields().get(1);
        assertEquals("score", scoreField.getName());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), scoreField.getType());

        Field nameField = schema.getFields().get(2);
        assertEquals("name", nameField.getName());
        assertEquals(ArrowType.Utf8.INSTANCE, nameField.getType());
    }

    // ---- INTEGER, FLOAT, BOOLEAN → correct Arrow types ----

    public void testIntegerFloatBoolean() {
        RelDataType rowType = typeFactory.builder()
            .add("count", SqlTypeName.INTEGER)
            .add("ratio", SqlTypeName.FLOAT)
            .add("active", SqlTypeName.BOOLEAN)
            .build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        assertEquals(3, schema.getFields().size());

        Field countField = schema.getFields().get(0);
        assertEquals("count", countField.getName());
        assertEquals(new ArrowType.Int(32, true), countField.getType());

        Field ratioField = schema.getFields().get(1);
        assertEquals("ratio", ratioField.getName());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), ratioField.getType());

        Field activeField = schema.getFields().get(2);
        assertEquals("active", activeField.getName());
        assertEquals(ArrowType.Bool.INSTANCE, activeField.getType());
    }

    // ---- All fields are nullable ----

    public void testAllFieldsNullable() {
        RelDataType rowType = typeFactory.builder()
            .add("id", SqlTypeName.BIGINT)
            .add("name", SqlTypeName.VARCHAR)
            .add("data", SqlTypeName.VARBINARY)
            .build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        for (Field field : schema.getFields()) {
            assertTrue("Field '" + field.getName() + "' should be nullable", field.isNullable());
        }
    }

    // ---- VARBINARY → Arrow Binary ----

    public void testVarbinaryMapsToArrowBinary() {
        RelDataType rowType = typeFactory.builder().add("payload", SqlTypeName.VARBINARY).build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        assertEquals(1, schema.getFields().size());
        assertEquals(ArrowType.Binary.INSTANCE, schema.getFields().get(0).getType());
    }

    // ---- Unsupported type throws ----

    public void testUnsupportedTypeThrows() {
        RelDataType rowType = typeFactory.builder().add("ts", SqlTypeName.TIMESTAMP).build();

        expectThrows(IllegalArgumentException.class, () -> ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType));
    }
}
