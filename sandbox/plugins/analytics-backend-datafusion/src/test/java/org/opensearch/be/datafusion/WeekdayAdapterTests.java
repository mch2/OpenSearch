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
 * Unit tests for {@link WeekdayAdapter}.
 *
 * <p>Asserts {@code WEEKDAY(ts) → (date_part('dow', ts) + 6) % 7} and that
 * arity mismatches throw rather than silently returning the original call.
 */
public class WeekdayAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction WEEKDAY = new SqlFunction(
        "WEEKDAY",
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

    public void testDecomposesToDowPlusSixModSeven() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, WEEKDAY, List.of(ts));

        RexNode adapted = new WeekdayAdapter().adapt(original, List.of(), cluster);

        RexCall outer = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.MOD, outer.getOperator());
        assertEquals(7L, ((RexLiteral) outer.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall shifted = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.PLUS, shifted.getOperator());
        assertEquals(6L, ((RexLiteral) shifted.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall dowCall = (RexCall) shifted.getOperands().get(0);
        assertEquals("DATE_PART", dowCall.getOperator().getName());
        assertEquals("dow", ((RexLiteral) dowCall.getOperands().get(0)).getValueAs(String.class));
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, WEEKDAY, List.of());

        expectThrows(IllegalArgumentException.class, () -> new WeekdayAdapter().adapt(original, List.of(), cluster));
    }
}
