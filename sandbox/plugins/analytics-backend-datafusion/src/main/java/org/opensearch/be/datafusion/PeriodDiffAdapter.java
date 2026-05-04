/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites {@code PERIOD_DIFF(P1, P2)} → {@code (P1/100)*12 + (P1 mod 100) - ((P2/100)*12 + (P2 mod 100))}.
 * P is YYYYMM (e.g. 200802 = Feb 2008).
 */
class PeriodDiffAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("PERIOD_DIFF expects 2 operands, got " + original.getOperands().size());
        }
        RexNode p1 = original.getOperands().get(0);
        RexNode p2 = original.getOperands().get(1);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType integer = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        RexNode months1 = periodToMonths(rb, integer, p1);
        RexNode months2 = periodToMonths(rb, integer, p2);
        return rb.makeCall(original.getType(), SqlStdOperatorTable.MINUS, List.of(months1, months2));
    }

    static RexNode periodToMonths(RexBuilder rb, RelDataType integer, RexNode p) {
        RexNode pInt = rb.makeCast(integer, p);
        RexNode hundred = rb.makeLiteral(BigDecimal.valueOf(100L), integer, false);
        RexNode twelve = rb.makeLiteral(BigDecimal.valueOf(12L), integer, false);
        RexNode year = rb.makeCall(integer, SqlStdOperatorTable.DIVIDE, List.of(pInt, hundred));
        RexNode month = rb.makeCall(integer, SqlStdOperatorTable.MOD, List.of(pInt, hundred));
        RexNode yearMonths = rb.makeCall(integer, SqlStdOperatorTable.MULTIPLY, List.of(year, twelve));
        return rb.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(yearMonths, month));
    }
}
