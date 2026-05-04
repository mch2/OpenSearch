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
 * Unit tests for {@link PeriodAddAdapter}.
 *
 * <p>Asserts the YYYYMM arithmetic decomposition shape and that arity mismatches throw.
 */
public class PeriodAddAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction PERIOD_ADD = new SqlFunction(
        "PERIOD_ADD",
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

    public void testTopLevelOpIsPlusOfYearScaledAndMonth() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode p = rexBuilder.makeInputRef(integer, 0);
        RexNode n = rexBuilder.makeInputRef(integer, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(integer, PERIOD_ADD, List.of(p, n));

        RexNode adapted = new PeriodAddAdapter().adapt(original, List.of(), cluster);

        // Final shape: (newYear*100) + newMonth
        RexCall plus = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, plus.getOperator());
    }

    public void testWrongArityThrows() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall original = (RexCall) rexBuilder.makeCall(
            integer, PERIOD_ADD, List.of(rexBuilder.makeInputRef(integer, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new PeriodAddAdapter().adapt(original, List.of(), cluster));
    }
}
