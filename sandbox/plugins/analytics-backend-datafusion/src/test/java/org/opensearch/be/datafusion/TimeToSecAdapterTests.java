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
 * Unit tests for {@link TimeToSecAdapter}.
 *
 * <p>Asserts {@code TIME_TO_SEC(t) → hour*3600 + minute*60 + second} via
 * {@code date_part} and that arity mismatches throw.
 */
public class TimeToSecAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TIME_TO_SEC = new SqlFunction(
        "TIME_TO_SEC",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
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

    public void testDecomposesToHourSecondsPlusMinuteSecondsPlusSecond() {
        RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode t = rexBuilder.makeInputRef(time, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TIME_TO_SEC, List.of(t));

        RexNode adapted = new TimeToSecAdapter().adapt(original, List.of(), cluster);

        // Outer: PLUS((hour*3600 + minute*60), second)
        RexCall outer = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, outer.getOperator());
        RexCall secondCall = (RexCall) outer.getOperands().get(1);
        assertEquals("DATE_PART", secondCall.getOperator().getName());
        assertEquals("second", ((RexLiteral) secondCall.getOperands().get(0)).getValueAs(String.class));
        RexCall inner = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.PLUS, inner.getOperator());
        RexCall hour3600 = (RexCall) inner.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MULTIPLY, hour3600.getOperator());
        assertEquals(3600L, ((RexLiteral) hour3600.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall minute60 = (RexCall) inner.getOperands().get(1);
        assertEquals(SqlStdOperatorTable.MULTIPLY, minute60.getOperator());
        assertEquals(60L, ((RexLiteral) minute60.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TIME_TO_SEC, List.of());

        expectThrows(IllegalArgumentException.class, () -> new TimeToSecAdapter().adapt(original, List.of(), cluster));
    }
}
