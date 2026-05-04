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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites {@code YEARWEEK(ts)} → {@code date_part('year', ts) * 100 + date_part('week', ts)}.
 * Approximates MySQL's YEARWEEK in mode 0 well enough for simple ISO-week cases; ignores the
 * optional {@code mode} argument (PPL passes it through but DataFusion has no variant).
 *
 * @opensearch.internal
 */
class YearweekAdapter implements ScalarFunctionAdapter {

    private static final SqlFunction DATE_PART = new SqlFunction(
        "DATE_PART",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().isEmpty()) {
            throw new IllegalArgumentException("YEARWEEK expects 1 operand, got " + original.getOperands().size());
        }
        RexNode operand = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode yearPart = rb.makeLiteral("year", varchar, false);
        RexNode weekPart = rb.makeLiteral("week", varchar, false);
        RexNode year = rb.makeCall(bigint, DATE_PART, List.of(yearPart, operand));
        RexNode week = rb.makeCall(bigint, DATE_PART, List.of(weekPart, operand));
        RexNode hundred = rb.makeLiteral(BigDecimal.valueOf(100L), bigint, false);
        RexNode yearShifted = rb.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(year, hundred));
        return rb.makeCall(original.getType(), SqlStdOperatorTable.PLUS, List.of(yearShifted, week));
    }
}
