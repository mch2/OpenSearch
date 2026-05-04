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
 * Unit tests for {@link FromDaysAdapter}.
 *
 * <p>Asserts {@code FROM_DAYS(n) → cast(from_unixtime((n - 719528) * 86400) AS date)}
 * and that arity mismatches throw.
 */
public class FromDaysAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction FROM_DAYS = new SqlFunction(
        "FROM_DAYS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE,
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

    public void testDecomposesToCastFromUnixtimeOffset() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType date = typeFactory.createSqlType(SqlTypeName.DATE);
        RexNode n = rexBuilder.makeInputRef(bigint, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(date, FROM_DAYS, List.of(n));

        RexNode adapted = new FromDaysAdapter().adapt(original, List.of(), cluster);

        // Outer is CAST to DATE; within it: FROM_UNIXTIME((n - 719528) * 86400)
        RexCall cast = (RexCall) adapted;
        assertEquals(SqlKind.CAST, cast.getKind());
        RexCall fromUnix = (RexCall) cast.getOperands().get(0);
        assertEquals("FROM_UNIXTIME", fromUnix.getOperator().getName());
        RexCall mul = (RexCall) fromUnix.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MULTIPLY, mul.getOperator());
        assertEquals(86400L, ((RexLiteral) mul.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall minus = (RexCall) mul.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MINUS, minus.getOperator());
        assertEquals(719528L, ((RexLiteral) minus.getOperands().get(1)).getValueAs(Long.class).longValue());
    }

    public void testWrongArityThrows() {
        RelDataType date = typeFactory.createSqlType(SqlTypeName.DATE);
        RexCall original = (RexCall) rexBuilder.makeCall(date, FROM_DAYS, List.of());

        expectThrows(IllegalArgumentException.class, () -> new FromDaysAdapter().adapt(original, List.of(), cluster));
    }
}
