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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites date-add/sub UDF calls to expressions DataFusion can evaluate.
 * <ul>
 *   <li>{@code ADDDATE/SUBDATE(ts, n_days)} → {@code from_unixtime(to_unixtime(ts) ± n*86400)}
 *       (DF rejects {@code timestamp + integer} arithmetic).</li>
 *   <li>{@code DATE_ADD/DATE_SUB(ts, INTERVAL ...)} → {@code ts ± interval}
 *       (substrait core declares {@code add(timestamp, interval)} → {@code timestamp}).</li>
 * </ul>
 *
 * @opensearch.internal
 */
class DateArithmeticAdapter implements ScalarFunctionAdapter {

    private static final SqlFunction TO_UNIXTIME = new SqlFunction(
        "TO_UNIXTIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    private static final SqlFunction FROM_UNIXTIME = new SqlFunction(
        "FROM_UNIXTIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.TIMEDATE
    );

    private final boolean add;

    DateArithmeticAdapter(boolean add) {
        this.add = add;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException(
                (add ? "ADDDATE/DATE_ADD" : "SUBDATE/DATE_SUB") + " expects 2 operands, got "
                    + original.getOperands().size()
            );
        }
        RexNode left = original.getOperands().get(0);
        RexNode right = original.getOperands().get(1);

        SqlTypeFamily rightFamily = right.getType().getSqlTypeName().getFamily();
        if (rightFamily == SqlTypeFamily.NUMERIC || SqlTypeName.INT_TYPES.contains(right.getType().getSqlTypeName())) {
            return rewriteAsEpochArithmetic(original, left, right, cluster);
        }
        // Interval (or anything else) → use direct +/- which substrait core supports.
        return cluster.getRexBuilder().makeCall(
            original.getType(),
            add ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS,
            List.of(left, right)
        );
    }

    /** {@code from_unixtime(to_unixtime(ts) ± n_days * 86400)} — pure scalar math.
     *  DataFusion doesn't accept {@code timestamp + integer}; the epoch round-trip
     *  uses the YAML-declared {@code to_unixtime} / {@code from_unixtime} which DF
     *  resolves to its built-in scalar functions. */
    private RexNode rewriteAsEpochArithmetic(RexCall original, RexNode ts, RexNode days, RelOptCluster cluster) {
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RelDataType nullableBigint = cluster.getTypeFactory().createTypeWithNullability(bigint, true);

        RexNode epochSeconds = rb.makeCall(nullableBigint, TO_UNIXTIME, List.of(ts));
        RexNode daysAsBigint = rb.makeCast(nullableBigint, days);
        RexNode secondsPerDay = rb.makeLiteral(BigDecimal.valueOf(86400L), bigint);
        RexNode deltaSeconds = rb.makeCall(
            nullableBigint,
            SqlStdOperatorTable.MULTIPLY,
            List.of(daysAsBigint, secondsPerDay)
        );
        RexNode shifted = rb.makeCall(
            nullableBigint,
            add ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS,
            List.of(epochSeconds, deltaSeconds)
        );
        return rb.makeCall(original.getType(), FROM_UNIXTIME, List.of(shifted));
    }
}
