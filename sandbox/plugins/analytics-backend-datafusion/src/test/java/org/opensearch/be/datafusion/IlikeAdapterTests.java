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
 * Unit tests for {@link IlikeAdapter}.
 *
 * <p>Asserts {@code ILIKE(a, b) → LIKE(LOWER(a), LOWER(b))} and that arity
 * mismatches throw.
 */
public class IlikeAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction ILIKE = new SqlFunction(
        "ILIKE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
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

    public void testRewritesToLowerLike() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType bool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RexNode input = rexBuilder.makeInputRef(varchar, 0);
        RexNode pattern = rexBuilder.makeInputRef(varchar, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(bool, ILIKE, List.of(input, pattern));

        RexNode adapted = new IlikeAdapter().adapt(original, List.of(), cluster);

        RexCall like = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.LIKE, like.getOperator());
        assertEquals(SqlStdOperatorTable.LOWER, ((RexCall) like.getOperands().get(0)).getOperator());
        assertEquals(SqlStdOperatorTable.LOWER, ((RexCall) like.getOperands().get(1)).getOperator());
    }

    public void testTooFewOperandsThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType bool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RexCall original = (RexCall) rexBuilder.makeCall(
            bool, ILIKE, List.of(rexBuilder.makeInputRef(varchar, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new IlikeAdapter().adapt(original, List.of(), cluster));
    }
}
