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

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link TimestampAddAdapter}.
 *
 * <p>Covers fixed-unit decomposition and verifies that calendar units
 * (MONTH/QUARTER/YEAR) throw an explicit IllegalArgumentException
 * pointing at Stream 3's {@code timestampadd_calendar} Rust UDF
 * rather than silently returning the original call.
 */
public class TimestampAddAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TIMESTAMPADD = new SqlFunction(
        "TIMESTAMPADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
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
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode u = rexBuilder.makeLiteral(unit, varchar, false);
        RexNode n = rexBuilder.makeLiteral(BigDecimal.valueOf(5L), integer, false);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        return (RexCall) rexBuilder.makeCall(timestamp, TIMESTAMPADD, List.of(u, n, ts));
    }

    public void testDayUnitDecomposesToEpochArithmetic() {
        RexCall original = buildCall("DAY");

        RexNode adapted = new TimestampAddAdapter().adapt(original, List.of(), cluster);

        RexCall fromUnix = (RexCall) adapted;
        assertEquals("FROM_UNIXTIME", fromUnix.getOperator().getName());
        RexCall plus = (RexCall) fromUnix.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.PLUS, plus.getOperator());
        assertEquals("TO_UNIXTIME", ((RexCall) plus.getOperands().get(0)).getOperator().getName());
        RexCall mul = (RexCall) plus.getOperands().get(1);
        assertEquals(SqlStdOperatorTable.MULTIPLY, mul.getOperator());
        assertEquals(86400L, ((RexLiteral) mul.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testCalendarUnitMonthThrows() {
        RexCall original = buildCall("MONTH");

        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
        assertTrue(thrown.getMessage().toLowerCase(java.util.Locale.ROOT).contains("calendar"));
    }

    public void testCalendarUnitQuarterThrows() {
        RexCall original = buildCall("QUARTER");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
    }

    public void testCalendarUnitYearThrows() {
        RexCall original = buildCall("YEAR");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
    }

    public void testUnknownUnitThrows() {
        RexCall original = buildCall("FORTNIGHT");

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
    }

    public void testWrongArityThrows() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexCall original = (RexCall) rexBuilder.makeCall(timestamp, TIMESTAMPADD, List.of());

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
    }

    public void testNonLiteralUnitThrows() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode u = rexBuilder.makeInputRef(varchar, 2);
        RexNode n = rexBuilder.makeLiteral(BigDecimal.valueOf(5L), integer, false);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(timestamp, TIMESTAMPADD, List.of(u, n, ts));

        expectThrows(IllegalArgumentException.class,
            () -> new TimestampAddAdapter().adapt(original, List.of(), cluster));
    }
}
