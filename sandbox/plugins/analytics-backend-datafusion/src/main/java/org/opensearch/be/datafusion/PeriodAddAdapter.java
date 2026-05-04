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
 * Rewrites {@code PERIOD_ADD(P, n)} where P is YYYYMM → compute
 * {@code total = (P/100)*12 + (P mod 100) - 1 + n}, then format as
 * {@code (total/12)*100 + (total mod 12) + 1}. Handles negative n.
 */
class PeriodAddAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("PERIOD_ADD expects 2 operands, got " + original.getOperands().size());
        }
        RexNode p = original.getOperands().get(0);
        RexNode n = original.getOperands().get(1);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType integer = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        RexNode pInt = rb.makeCast(integer, p);
        RexNode nInt = rb.makeCast(integer, n);
        RexNode hundred = rb.makeLiteral(BigDecimal.valueOf(100L), integer, false);
        RexNode twelve = rb.makeLiteral(BigDecimal.valueOf(12L), integer, false);
        RexNode one = rb.makeLiteral(BigDecimal.valueOf(1L), integer, false);
        RexNode year = rb.makeCall(integer, SqlStdOperatorTable.DIVIDE, List.of(pInt, hundred));
        RexNode month = rb.makeCall(integer, SqlStdOperatorTable.MOD, List.of(pInt, hundred));
        RexNode yearMonths = rb.makeCall(integer, SqlStdOperatorTable.MULTIPLY, List.of(year, twelve));
        RexNode monthsSinceEra = rb.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(yearMonths, month));
        RexNode zeroIndexed = rb.makeCall(integer, SqlStdOperatorTable.MINUS, List.of(monthsSinceEra, one));
        RexNode total = rb.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(zeroIndexed, nInt));
        RexNode newYear = rb.makeCall(integer, SqlStdOperatorTable.DIVIDE, List.of(total, twelve));
        RexNode newMonthZero = rb.makeCall(integer, SqlStdOperatorTable.MOD, List.of(total, twelve));
        RexNode newMonth = rb.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(newMonthZero, one));
        RexNode yearPart = rb.makeCall(integer, SqlStdOperatorTable.MULTIPLY, List.of(newYear, hundred));
        return rb.makeCall(original.getType(), SqlStdOperatorTable.PLUS, List.of(yearPart, newMonth));
    }
}
