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
 * Rewrites {@code DATEDIFF(ts1, ts2)} → {@code (to_unixtime(ts1) - to_unixtime(ts2)) / 86400}.
 * DataFusion's substrait consumer does not recognise {@code date_diff}; this epoch-subtraction
 * form reduces to arithmetic on BIGINT seconds and gives the whole-day difference.
 *
 * @opensearch.internal
 */
class DateDiffAdapter implements ScalarFunctionAdapter {

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
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("DATEDIFF expects 2 operands, got " + original.getOperands().size());
        }
        RexNode ts1 = original.getOperands().get(0);
        RexNode ts2 = original.getOperands().get(1);

        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode e1 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts1));
        RexNode e2 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts2));
        RexNode diff = rb.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(e1, e2));
        RexNode secondsPerDay = rb.makeLiteral(BigDecimal.valueOf(86400L), bigint, false);
        return rb.makeCall(original.getType(), SqlStdOperatorTable.DIVIDE, List.of(diff, secondsPerDay));
    }
}
