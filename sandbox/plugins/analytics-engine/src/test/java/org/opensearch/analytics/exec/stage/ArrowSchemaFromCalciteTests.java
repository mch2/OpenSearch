/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Per-type coverage of {@link ArrowSchemaFromCalcite#fieldFromCalcite(RelDataTypeField)}.
 *
 * <p>Each test pins the Arrow type we emit for a given Calcite {@link SqlTypeName} to the
 * convention already used elsewhere in the plugin stack (notably
 * {@code DateParquetField} / {@code DateNanosParquetField}) so the mid-stage input
 * schema agrees with what the data node's reader vectors actually carry.
 */
public class ArrowSchemaFromCalciteTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    public void testTimestampIsMilliTimestamp() {
        Field field = fieldFor(SqlTypeName.TIMESTAMP);
        assertEquals(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), field.getType());
    }

    public void testDateIsDayDate() {
        Field field = fieldFor(SqlTypeName.DATE);
        assertEquals(new ArrowType.Date(DateUnit.DAY), field.getType());
    }

    public void testTimeIsMilliTime32() {
        Field field = fieldFor(SqlTypeName.TIME);
        assertEquals(new ArrowType.Time(TimeUnit.MILLISECOND, 32), field.getType());
    }

    public void testDecimalCarriesPrecisionAndScale() {
        RelDataType relType = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        Field field = ArrowSchemaFromCalcite.fieldFromCalcite(new RelDataTypeFieldImpl("col", 0, relType));
        ArrowType.Decimal expected = new ArrowType.Decimal(10, 2, 128);
        assertEquals(expected, field.getType());
    }

    private Field fieldFor(SqlTypeName sqlTypeName) {
        RelDataType relType = typeFactory.createSqlType(sqlTypeName);
        return ArrowSchemaFromCalcite.fieldFromCalcite(new RelDataTypeFieldImpl("col", 0, relType));
    }
}
