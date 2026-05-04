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
 * Unit tests for {@link SecToTimeAdapter}.
 *
 * <p>Asserts {@code SEC_TO_TIME(s) → make_time(s/3600, (s%3600)/60, s%60)} and
 * that arity mismatches throw.
 */
public class SecToTimeAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction SEC_TO_TIME = new SqlFunction(
        "SEC_TO_TIME",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME,
        null,
        OperandTypes.NUMERIC,
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

    public void testDecomposesToMakeTime() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode s = rexBuilder.makeInputRef(bigint, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(time, SEC_TO_TIME, List.of(s));

        RexNode adapted = new SecToTimeAdapter().adapt(original, List.of(), cluster);

        RexCall outer = (RexCall) adapted;
        assertEquals("MAKE_TIME", outer.getOperator().getName());
        assertEquals(3, outer.getOperands().size());
        // hour = s / 3600
        RexCall hour = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.DIVIDE, hour.getOperator());
        assertEquals(3600L, ((RexLiteral) hour.getOperands().get(1)).getValueAs(Long.class).longValue());
        // minute = (s % 3600) / 60
        RexCall minute = (RexCall) outer.getOperands().get(1);
        assertEquals(SqlStdOperatorTable.DIVIDE, minute.getOperator());
        assertEquals(60L, ((RexLiteral) minute.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall minuteMod = (RexCall) minute.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MOD, minuteMod.getOperator());
        assertEquals(3600L, ((RexLiteral) minuteMod.getOperands().get(1)).getValueAs(Long.class).longValue());
        // second = s % 60
        RexCall second = (RexCall) outer.getOperands().get(2);
        assertEquals(SqlStdOperatorTable.MOD, second.getOperator());
        assertEquals(60L, ((RexLiteral) second.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testWrongArityThrows() {
        RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
        RexCall original = (RexCall) rexBuilder.makeCall(time, SEC_TO_TIME, List.of());

        expectThrows(IllegalArgumentException.class, () -> new SecToTimeAdapter().adapt(original, List.of(), cluster));
    }
}
