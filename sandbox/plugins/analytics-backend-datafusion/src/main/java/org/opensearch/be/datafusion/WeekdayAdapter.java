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
 * Rewrites {@code WEEKDAY(ts)} → {@code (date_part('dow', ts) + 6) % 7}.
 * DataFusion's {@code date_part('dow', x)} returns Sunday=0..Saturday=6, matching Postgres.
 * PPL's WEEKDAY returns Monday=0..Sunday=6 (MySQL convention), so shift by 6 and wrap mod 7.
 *
 * @opensearch.internal
 */
class WeekdayAdapter implements ScalarFunctionAdapter {

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
            throw new IllegalArgumentException("WEEKDAY expects 1 operand, got " + original.getOperands().size());
        }
        RexNode operand = original.getOperands().get(0);
        RexBuilder rb = cluster.getRexBuilder();
        RelDataType varchar = cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode part = rb.makeLiteral("dow", varchar, false);
        RexNode dow = rb.makeCall(bigint, DATE_PART, List.of(part, operand));
        RexNode six = rb.makeLiteral(BigDecimal.valueOf(6L), bigint, false);
        RexNode seven = rb.makeLiteral(BigDecimal.valueOf(7L), bigint, false);
        RexNode shifted = rb.makeCall(bigint, SqlStdOperatorTable.PLUS, List.of(dow, six));
        return rb.makeCall(original.getType(), SqlStdOperatorTable.MOD, List.of(shifted, seven));
    }
}
