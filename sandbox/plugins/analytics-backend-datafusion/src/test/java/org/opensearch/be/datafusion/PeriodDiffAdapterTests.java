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
 * Unit tests for {@link PeriodDiffAdapter}.
 *
 * <p>Asserts MINUS over the month-counts and that arity mismatches throw.
 */
public class PeriodDiffAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction PERIOD_DIFF = new SqlFunction(
        "PERIOD_DIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
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

    public void testTopLevelOpIsMinus() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode p1 = rexBuilder.makeInputRef(integer, 0);
        RexNode p2 = rexBuilder.makeInputRef(integer, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(integer, PERIOD_DIFF, List.of(p1, p2));

        RexNode adapted = new PeriodDiffAdapter().adapt(original, List.of(), cluster);

        RexCall minus = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.MINUS, minus.getOperator());
    }

    public void testWrongArityThrows() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall original = (RexCall) rexBuilder.makeCall(
            integer, PERIOD_DIFF, List.of(rexBuilder.makeInputRef(integer, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new PeriodDiffAdapter().adapt(original, List.of(), cluster));
    }
}
