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
 * Unit tests for {@link DateArithmeticAdapter}.
 *
 * <p>Covers both paths:
 * <ul>
 *   <li>Numeric rhs → {@code from_unixtime(to_unixtime(ts) ± n*86400)} (epoch arithmetic).</li>
 *   <li>Interval rhs → direct {@code ts + interval} (substrait core handles it).</li>
 * </ul>
 * Arity mismatches throw.
 */
public class DateArithmeticAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction DATE_ADD = new SqlFunction(
        "DATE_ADD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP,
        null,
        OperandTypes.ANY_ANY,
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

    public void testNumericRhsRewritesToEpochArithmetic() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexNode days = rexBuilder.makeLiteral(BigDecimal.valueOf(7L), integer, false);
        RexCall original = (RexCall) rexBuilder.makeCall(timestamp, DATE_ADD, List.of(ts, days));

        RexNode adapted = new DateArithmeticAdapter(true).adapt(original, List.of(), cluster);

        // Top-level: FROM_UNIXTIME(PLUS(TO_UNIXTIME(ts), MULTIPLY(cast(days), 86400)))
        RexCall fromUnix = (RexCall) adapted;
        assertEquals("FROM_UNIXTIME", fromUnix.getOperator().getName());
        RexCall plus = (RexCall) fromUnix.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.PLUS, plus.getOperator());
        assertEquals("TO_UNIXTIME", ((RexCall) plus.getOperands().get(0)).getOperator().getName());
        RexCall mul = (RexCall) plus.getOperands().get(1);
        assertEquals(SqlStdOperatorTable.MULTIPLY, mul.getOperator());
        assertEquals(86400L, ((RexLiteral) mul.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testSubtractionUsesMinus() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexNode days = rexBuilder.makeLiteral(BigDecimal.valueOf(3L), integer, false);
        RexCall original = (RexCall) rexBuilder.makeCall(timestamp, DATE_ADD, List.of(ts, days));

        RexNode adapted = new DateArithmeticAdapter(false).adapt(original, List.of(), cluster);

        RexCall fromUnix = (RexCall) adapted;
        RexCall combine = (RexCall) fromUnix.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MINUS, combine.getOperator());
    }

    public void testNonNumericRhsUsesDirectPlus() {
        // Any non-numeric rhs (e.g. a second timestamp) must not be wrapped in the epoch
        // round-trip — the adapter emits the direct PLUS/MINUS call that substrait-core
        // declares for timestamp ± interval.
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexNode rhs = rexBuilder.makeInputRef(timestamp, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(timestamp, DATE_ADD, List.of(ts, rhs));

        RexNode adapted = new DateArithmeticAdapter(true).adapt(original, List.of(), cluster);

        RexCall call = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, call.getOperator());
        assertSame(ts, call.getOperands().get(0));
        // The rhs operand is passed through unchanged — no TO_UNIXTIME wrapper.
        assertSame(rhs, call.getOperands().get(1));
    }

    public void testWrongArityThrows() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RexCall original = (RexCall) rexBuilder.makeCall(
            timestamp, DATE_ADD, List.of(rexBuilder.makeInputRef(timestamp, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new DateArithmeticAdapter(true).adapt(original, List.of(), cluster));
    }
}
