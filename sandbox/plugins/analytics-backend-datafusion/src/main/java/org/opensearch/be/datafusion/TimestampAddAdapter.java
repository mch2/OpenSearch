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
 * Rewrites {@code TIMESTAMPADD('UNIT', n, ts)} into epoch arithmetic:
 * {@code from_unixtime(to_unixtime(ts) + n * unit_seconds)}. PPL emits the unit
 * as an uppercase {@code VARCHAR} literal: SECOND, MINUTE, HOUR, DAY, WEEK.
 *
 * <p>Decomposition rationale: pure scalar arithmetic — DF's optimizer can hoist the
 * {@code to_unixtime/from_unixtime} conversions across multiple calls over the same
 * timestamp column. Routing through a UDF would lose this.
 *
 * <p>MILLISECOND/MICROSECOND lose sub-second precision through this round-trip and are
 * currently unsupported. Calendar units (MONTH / QUARTER / YEAR) have variable second
 * counts and cannot decompose — the adapter explicitly throws rather than silently
 * passing through (invariant #5). Stream 3 ships a {@code timestampadd_calendar} Rust
 * UDF for these; once that lands on subtraitupdates this adapter's calendar branch
 * should rewrite into a UDF call targeting that name.
 *
 * @opensearch.internal
 */
class TimestampAddAdapter implements ScalarFunctionAdapter {

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
        if (original.getOperands().size() != 3) {
            throw new IllegalArgumentException(
                "TIMESTAMPADD expects 3 operands (unit, n, ts), got " + original.getOperands().size()
            );
        }
        RexNode unitNode = original.getOperands().get(0);
        RexNode amountNode = original.getOperands().get(1);
        RexNode tsNode = original.getOperands().get(2);

        if (!(unitNode instanceof RexLiteral)) {
            throw new IllegalArgumentException("TIMESTAMPADD unit must be a literal");
        }
        String unit = unitStringOf((RexLiteral) unitNode);
        if (unit == null) {
            throw new IllegalArgumentException("TIMESTAMPADD unit literal is null");
        }
        String normalized = unit.toUpperCase(Locale.ROOT);
        if (CALENDAR_UNITS.contains(normalized)) {
            // TODO(stream-3-merge): Stream 3 ships `timestampadd_calendar` Rust UDF; once it
            // lands on subtraitupdates, rewrite this branch to emit a RexCall targeting that
            // UDF instead of throwing. Tracked as a Phase-2 follow-up task.
            throw new IllegalArgumentException(
                "TIMESTAMPADD calendar unit " + normalized + " requires the timestampadd_calendar Rust UDF (Stream 3)"
            );
        }
        Long unitSeconds = UNIT_TO_SECONDS.get(normalized);
        if (unitSeconds == null) {
            throw new IllegalArgumentException(
                "TIMESTAMPADD unit " + normalized
                    + " is not supported — fixed units are SECOND/MINUTE/HOUR/DAY/WEEK; calendar units MONTH/QUARTER/YEAR route through Stream 3's timestampadd_calendar UDF"
            );
        }

        RexBuilder rb = cluster.getRexBuilder();
        RelDataType bigint = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode epochSeconds = rb.makeCall(bigint, TO_UNIXTIME, List.of(tsNode));
        RexNode amountAsBigint = rb.makeCast(bigint, amountNode);
        RexNode unitLit = rb.makeLiteral(BigDecimal.valueOf(unitSeconds), bigint, false);
        RexNode delta = rb.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(amountAsBigint, unitLit));
        RexNode newEpoch = rb.makeCall(bigint, SqlStdOperatorTable.PLUS, List.of(epochSeconds, delta));
        return rb.makeCall(original.getType(), FROM_UNIXTIME, List.of(newEpoch));
    }

    private static String unitStringOf(RexLiteral lit) {
        Object v = lit.getValue();
        if (v instanceof NlsString) return ((NlsString) v).getValue();
        if (v instanceof String) return (String) v;
        return null;
    }
}
