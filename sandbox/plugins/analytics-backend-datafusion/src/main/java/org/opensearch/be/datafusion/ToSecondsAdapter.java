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
 * Rewrites {@code TO_SECONDS(ts)} → {@code to_unixtime(ts) + 62167219200}.
 * MySQL's second-count is measured from year 0 (719528 days before 1970-01-01 =
 * 62167219200 seconds).
 */
class ToSecondsAdapter implements ScalarFunctionAdapter {

    private static final long EPOCH_SECOND_OFFSET = 62167219200L;

    private static final SqlFunction TO_UNIXTIME = new SqlFunction(
        "TO_UNIXTIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("TO_SECONDS expects 1 operand, got " + original.getOperands().size());
        }
        RexNode ts = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode epoch = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts));
        RexNode offset = rb.makeLiteral(BigDecimal.valueOf(EPOCH_SECOND_OFFSET), bigint, false);
        return rb.makeCall(original.getType(), SqlStdOperatorTable.PLUS, List.of(epoch, offset));
    }
}
