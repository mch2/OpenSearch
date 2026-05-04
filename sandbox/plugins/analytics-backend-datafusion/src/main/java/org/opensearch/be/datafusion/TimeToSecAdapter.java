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
 * Rewrites {@code TIME_TO_SEC(t)} → {@code hour*3600 + minute*60 + second} using
 * {@code date_part('hour|minute|second', t)}.
 *
 * @opensearch.internal
 */
class TimeToSecAdapter implements ScalarFunctionAdapter {

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
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("TIME_TO_SEC expects 1 operand, got " + original.getOperands().size());
        }
        RexNode operand = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode hourPart = rb.makeLiteral("hour", varchar, false);
        RexNode minutePart = rb.makeLiteral("minute", varchar, false);
        RexNode secondPart = rb.makeLiteral("second", varchar, false);
        RexNode hour = rb.makeCall(bigint, DATE_PART, List.of(hourPart, operand));
        RexNode minute = rb.makeCall(bigint, DATE_PART, List.of(minutePart, operand));
        RexNode second = rb.makeCall(bigint, DATE_PART, List.of(secondPart, operand));
        RexNode h3600 = rb.makeLiteral(BigDecimal.valueOf(3600L), bigint, false);
        RexNode m60 = rb.makeLiteral(BigDecimal.valueOf(60L), bigint, false);
        RexNode hourSecs = rb.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(hour, h3600));
        RexNode minuteSecs = rb.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(minute, m60));
        RexNode hourPlusMinute = rb.makeCall(bigint, SqlStdOperatorTable.PLUS, List.of(hourSecs, minuteSecs));
        return rb.makeCall(original.getType(), SqlStdOperatorTable.PLUS, List.of(hourPlusMinute, second));
    }
}
