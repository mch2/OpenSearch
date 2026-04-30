/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code tostring(x)} → {@code CAST(x AS VARCHAR)}. PPL emits
 * {@code tostring(...)} as a UDF call; the semantics map directly to a CAST.
 * Doing the rewrite at the BackendPlanAdapter stage keeps the PPL frontend
 * backend-agnostic.
 *
 * @opensearch.internal
 */
class ToStringAdapter implements ScalarFunctionAdapter {

    private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final RelDataType VARCHAR_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    private static final RelDataType NULLABLE_VARCHAR_TYPE = TYPE_FACTORY.createTypeWithNullability(VARCHAR_TYPE, true);

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage) {
        if (original.getOperands().size() != 1) return original;
        RexNode operand = original.getOperands().get(0);
        RelDataType target = operand.getType().isNullable() ? NULLABLE_VARCHAR_TYPE : VARCHAR_TYPE;
        return REX_BUILDER.makeCast(target, operand);
    }
}
