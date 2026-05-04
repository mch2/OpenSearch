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
 * Rewrites {@code FROM_DAYS(n)} → {@code cast(from_unixtime((n - 719528) * 86400) as date)}.
 * Inverse of {@link ToDaysAdapter} — n is days since year 0.
 */
class FromDaysAdapter implements ScalarFunctionAdapter {

    private static final long EPOCH_DAY_OFFSET = 719528L;

    private static final SqlFunction FROM_UNIXTIME = new SqlFunction(
        "FROM_UNIXTIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("FROM_DAYS expects 1 operand, got " + original.getOperands().size());
        }
        RexNode n = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode nBig = rb.makeCast(bigint, n);
        RexNode offset = rb.makeLiteral(BigDecimal.valueOf(EPOCH_DAY_OFFSET), bigint, false);
        RexNode daysSinceEpoch = rb.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(nBig, offset));
        RexNode secondsPerDay = rb.makeLiteral(BigDecimal.valueOf(86400L), bigint, false);
        RexNode epochSeconds = rb.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(daysSinceEpoch, secondsPerDay));
        RelDataType ts = cluster.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
        RexNode timestamp = rb.makeCall(ts, FROM_UNIXTIME, List.of(epochSeconds));
        return rb.makeCast(original.getType(), timestamp);
    }
}
