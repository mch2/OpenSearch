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
 * Unit tests for {@link StrcmpAdapter}.
 *
 * <p>Asserts {@code strcmp(a, b) → CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END}
 * and that arity mismatches throw.
 */
public class StrcmpAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction STRCMP = new SqlFunction(
        "STRCMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    public void testDecomposesToCase() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode a = rexBuilder.makeInputRef(varchar, 0);
        RexNode b = rexBuilder.makeInputRef(varchar, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(integer, STRCMP, List.of(a, b));

        RexNode adapted = new StrcmpAdapter().adapt(original, List.of(), cluster);

        RexCall caseCall = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.CASE, caseCall.getOperator());
        // shape: CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END → 5 operands
        assertEquals(5, caseCall.getOperands().size());
        RexCall less = (RexCall) caseCall.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.LESS_THAN, less.getOperator());
        RexCall greater = (RexCall) caseCall.getOperands().get(2);
        assertEquals(SqlStdOperatorTable.GREATER_THAN, greater.getOperator());
    }

    public void testWrongArityThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall original = (RexCall) rexBuilder.makeCall(
            integer, STRCMP, List.of(rexBuilder.makeInputRef(varchar, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new StrcmpAdapter().adapt(original, List.of(), cluster));
    }
}
