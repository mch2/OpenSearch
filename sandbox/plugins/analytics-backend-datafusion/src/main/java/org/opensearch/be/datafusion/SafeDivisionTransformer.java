/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code a / b} → {@code CASE WHEN b = 0 THEN NULL ELSE a / b END}
 * and {@code a % b} → same pattern. DataFusion's Arrow kernel throws on
 * divide-by-zero; PPL expects null (MySQL semantics).
 *
 * @opensearch.internal
 */
class SafeDivisionTransformer implements ScalarFunctionAdapter {

    private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage) {
        if (original.getOperands().size() != 2) return original;

        RexNode right = original.getOperands().get(1);

        RexNode zero = REX_BUILDER.makeLiteral(0, right.getType(), false);
        RexNode isZero = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, right, zero);
        RexNode nullLit = REX_BUILDER.makeNullLiteral(original.getType());

        return REX_BUILDER.makeCall(
            SqlStdOperatorTable.CASE,
            List.of(isZero, nullLit, original)
        );
    }
}
