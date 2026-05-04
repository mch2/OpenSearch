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
 * Rewrites {@code SEC_TO_TIME(s)} → {@code make_time(s/3600, (s%3600)/60, s%60)}.
 *
 * @opensearch.internal
 */
class SecToTimeAdapter implements ScalarFunctionAdapter {

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
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("SEC_TO_TIME expects 1 operand, got " + original.getOperands().size());
        }
        RexNode seconds = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode secondsBig = rb.makeCast(bigint, seconds);
        RexNode sixty = rb.makeLiteral(BigDecimal.valueOf(60L), bigint, false);
        RexNode thirtySixHundred = rb.makeLiteral(BigDecimal.valueOf(3600L), bigint, false);
        RexNode hour = rb.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(secondsBig, thirtySixHundred));
        RexNode hourRem = rb.makeCall(bigint, SqlStdOperatorTable.MOD, List.of(secondsBig, thirtySixHundred));
        RexNode minute = rb.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(hourRem, sixty));
        RexNode second = rb.makeCall(bigint, SqlStdOperatorTable.MOD, List.of(secondsBig, sixty));
        return rb.makeCall(original.getType(), MAKE_TIME, List.of(hour, minute, second));
    }
}
