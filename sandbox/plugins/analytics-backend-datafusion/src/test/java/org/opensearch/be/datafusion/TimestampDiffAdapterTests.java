/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link TimestampDiffAdapter}.
 *
 * <p>Covers fixed-unit decomposition and verifies that calendar units
 * (MONTH/QUARTER/YEAR) throw an explicit IllegalArgumentException
 * pointing at Stream 3's {@code timestampdiff_calendar} Rust UDF
 * rather than silently returning the original call.
 */
public class TimestampDiffAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TIMESTAMPDIFF = new SqlFunction(
        "TIMESTAMPDIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    private RexCall buildCall(String unit) {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode u = rexBuilder.makeLiteral(unit, varchar, false);
        RexNode a = rexBuilder.makeInputRef(timestamp, 0);
        RexNode b = rexBuilder.makeInputRef(timestamp, 1);
        return (RexCall) rexBuilder.makeCall(bigint, TIMESTAMPDIFF, List.of(u, a, b));
    }

    public void testDayUnitDividesBy86400() {
        RexCall original = buildCall("DAY");

        RexNode adapted = new TimestampDiffAdapter().adapt(original, List.of(), cluster);

        RexCall div = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.DIVIDE, div.getOperator());
        assertEquals(86400L, ((RexLiteral) div.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testSecondUnitIsCastWithoutDivide() {
        RexCall original = buildCall("SECOND");

        RexNode adapted = new TimestampDiffAdapter().adapt(original, List.of(), cluster);

        // Second is delta / 1 → the adapter just casts to result type without DIVIDE.
        RexCall cast = (RexCall) adapted;
        assertEquals(SqlKind.CAST, cast.getKind());
    }

    public void testMillisecondMultipliesBy1000() {
        RexCall original = buildCall("MILLISECOND");

        RexNode adapted = new TimestampDiffAdapter().adapt(original, List.of(), cluster);

        RexCall mul = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.MULTIPLY, mul.getOperator());
        assertEquals(1000L, ((RexLiteral) mul.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testCalendarUnitMonthThrows() {
        RexCall original = buildCall("MONTH");

        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
        assertTrue("message mentions calendar-unit routing", thrown.getMessage().toLowerCase(java.util.Locale.ROOT).contains("calendar"));
    }

    public void testCalendarUnitQuarterThrows() {
        RexCall original = buildCall("QUARTER");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
    }

    public void testCalendarUnitYearThrows() {
        RexCall original = buildCall("YEAR");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
    }

    public void testUnknownUnitThrows() {
        RexCall original = buildCall("FORTNIGHT");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TIMESTAMPDIFF, List.of());

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
    }

    public void testNonLiteralUnitThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode u = rexBuilder.makeInputRef(varchar, 2);
        RexNode a = rexBuilder.makeInputRef(timestamp, 0);
        RexNode b = rexBuilder.makeInputRef(timestamp, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TIMESTAMPDIFF, List.of(u, a, b));

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampDiffAdapter().adapt(original, List.of(), cluster));
    }
}
