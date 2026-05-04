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
 * Rewrites {@code TIMEDIFF(a, b)} → {@code make_time(d/3600, (d%3600)/60, d%60)} where
 * {@code d = to_unixtime(a) - to_unixtime(b)}. PPL's TIMEDIFF returns a TIME-typed delta;
 * DataFusion has no time-subtraction primitive, so we synthesize a TIME value from the
 * epoch-second delta, matching the output type of the original call.
 *
 * @opensearch.internal
 */
class TimeDiffAdapter implements ScalarFunctionAdapter {

    private static final SqlFunction TO_UNIXTIME = new SqlFunction(
        "TO_UNIXTIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    private static final SqlFunction MAKE_TIME = new SqlFunction(
        "MAKE_TIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME,
        null,
        null,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("TIMEDIFF expects 2 operands, got " + original.getOperands().size());
        }
        RexNode ts1 = original.getOperands().get(0);
        RexNode ts2 = original.getOperands().get(1);

        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode e1 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts1));
        RexNode e2 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts2));
        RexNode delta = rb.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(e1, e2));
        RexNode sixty = rb.makeLiteral(BigDecimal.valueOf(60L), bigint, false);
        RexNode thirtySixHundred = rb.makeLiteral(BigDecimal.valueOf(3600L), bigint, false);
        RexNode hour = rb.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(delta, thirtySixHundred));
        RexNode hourRem = rb.makeCall(bigint, SqlStdOperatorTable.MOD, List.of(delta, thirtySixHundred));
        RexNode minute = rb.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(hourRem, sixty));
        RexNode second = rb.makeCall(bigint, SqlStdOperatorTable.MOD, List.of(delta, sixty));
        return rb.makeCall(original.getType(), MAKE_TIME, List.of(hour, minute, second));
    }
}
