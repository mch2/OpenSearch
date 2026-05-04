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
 * Unit tests for {@link MinuteOfDayAdapter}.
 *
 * <p>Asserts {@code MINUTE_OF_DAY(ts) → date_part('hour', ts)*60 + date_part('minute', ts)}
 * and that arity mismatches throw.
 */
public class MinuteOfDayAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction MINUTE_OF_DAY = new SqlFunction(
        "MINUTE_OF_DAY",
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

    public void testDecomposesToHourTimes60PlusMinute() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, MINUTE_OF_DAY, List.of(ts));

        RexNode adapted = new MinuteOfDayAdapter().adapt(original, List.of(), cluster);

        RexCall outer = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, outer.getOperator());
        RexCall hourTimes60 = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MULTIPLY, hourTimes60.getOperator());
        assertEquals(60L, ((RexLiteral) hourTimes60.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall hourCall = (RexCall) hourTimes60.getOperands().get(0);
        assertEquals("DATE_PART", hourCall.getOperator().getName());
        assertEquals("hour", ((RexLiteral) hourCall.getOperands().get(0)).getValueAs(String.class));
        RexCall minuteCall = (RexCall) outer.getOperands().get(1);
        assertEquals("DATE_PART", minuteCall.getOperator().getName());
        assertEquals("minute", ((RexLiteral) minuteCall.getOperands().get(0)).getValueAs(String.class));
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, MINUTE_OF_DAY, List.of());

        expectThrows(IllegalArgumentException.class, () -> new MinuteOfDayAdapter().adapt(original, List.of(), cluster));
    }
}
