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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Rewrites {@code TIMESTAMPDIFF('UNIT', ts1, ts2)} into epoch-difference arithmetic.
 * Semantics follow SQL: result = (ts2 - ts1) in {@code UNIT}.
 *
 * <p>Fixed units: {@code (to_unixtime(ts2) - to_unixtime(ts1)) / unit_seconds}. Integer
 * division truncates toward zero, which matches {@code TIMESTAMPDIFF} for positive
 * deltas. For {@code MILLISECOND}, the epoch delta is multiplied by {@code 1000} —
 * this must happen BEFORE any cast/floor so that sub-second differences are preserved.
 *
 * <p>Decomposition rationale: fixed-unit seconds are pure integer arithmetic that DF's
 * optimizer can CSE with any sibling {@code to_unixtime} calls on the same timestamp
 * column. Routing them through a UDF would lose that optimization.
 *
 * <p>Calendar units (MONTH / QUARTER / YEAR) have variable second counts and cannot be
 * decomposed into scalar arithmetic — the adapter explicitly throws rather than silently
 * passing through (invariant #5). Stream 3 ships a {@code timestampdiff_calendar} Rust
 * UDF for these; once that lands on subtraitupdates this adapter's calendar branch should
 * rewrite into a UDF call targeting that name.
 *
 * @opensearch.internal
 */
class TimestampDiffAdapter implements ScalarFunctionAdapter {

    private static final Map<String, Long> UNIT_TO_SECONDS = Map.of(
        "SECOND", 1L,
        "MINUTE", 60L,
        "HOUR", 3600L,
        "DAY", 86400L,
        "WEEK", 604800L
    );

    private static final java.util.Set<String> CALENDAR_UNITS = java.util.Set.of("MONTH", "QUARTER", "YEAR");

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
        if (original.getOperands().size() != 3) {
            throw new IllegalArgumentException(
                "TIMESTAMPDIFF expects 3 operands (unit, ts1, ts2), got " + original.getOperands().size()
            );
        }
        RexNode unitNode = original.getOperands().get(0);
        RexNode ts1Node = original.getOperands().get(1);
        RexNode ts2Node = original.getOperands().get(2);

        if (!(unitNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("TIMESTAMPDIFF unit must be a literal");
        }
        String unit = unitStringOf((RexLiteral) unitNode);
        if (unit == null) {
            throw new IllegalArgumentException("TIMESTAMPDIFF unit literal is null");
        }
        String normalized = unit.toUpperCase(Locale.ROOT);

        if (CALENDAR_UNITS.contains(normalized)) {
            // TODO(stream-3-merge): Stream 3 ships `timestampdiff_calendar` Rust UDF; once it
            // lands on subtraitupdates, rewrite this branch to emit a RexCall targeting that
            // UDF instead of throwing. Tracked as a Phase-2 follow-up task.
            throw new IllegalArgumentException(
                "TIMESTAMPDIFF calendar unit " + normalized + " requires the timestampdiff_calendar Rust UDF (Stream 3)"
            );
        }

        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode epoch1 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts1Node));
        RexNode epoch2 = rb.makeCall(bigint, TO_UNIXTIME, List.of(ts2Node));
        RexNode deltaSeconds = rb.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(epoch2, epoch1));

        if ("MILLISECOND".equals(normalized)) {
            RexNode thousand = rb.makeLiteral(BigDecimal.valueOf(1000L), bigint, false);
            return rb.makeCall(original.getType(), SqlStdOperatorTable.MULTIPLY, List.of(deltaSeconds, thousand));
        }

        Long unitSeconds = UNIT_TO_SECONDS.get(normalized);
        if (unitSeconds == null) {
            throw new IllegalArgumentException(
                "TIMESTAMPDIFF unit " + normalized
                    + " is not supported — fixed units are SECOND/MINUTE/HOUR/DAY/WEEK/MILLISECOND; calendar units MONTH/QUARTER/YEAR route through Stream 3's timestampdiff_calendar UDF"
            );
        }
        if (unitSeconds == 1L) {
            return rb.makeCast(original.getType(), deltaSeconds);
        }
        RexNode unitLit = rb.makeLiteral(BigDecimal.valueOf(unitSeconds), bigint, false);
        return rb.makeCall(original.getType(), SqlStdOperatorTable.DIVIDE, List.of(deltaSeconds, unitLit));
    }

    private static String unitStringOf(RexLiteral lit) {
        Object v = lit.getValue();
        if (v instanceof NlsString) return ((NlsString) v).getValue();
        if (v instanceof String) return (String) v;
        return null;
    }
}
