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
 * Unit tests for {@link DateDiffAdapter}.
 *
 * <p>Asserts {@code DATEDIFF(a, b) → (to_unixtime(a) - to_unixtime(b)) / 86400}
 * and that arity mismatches throw.
 */
public class DateDiffAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction DATEDIFF = new SqlFunction(
        "DATEDIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
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

    public void testDecomposesToEpochDiffDividedByDaySeconds() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode a = rexBuilder.makeInputRef(timestamp, 0);
        RexNode b = rexBuilder.makeInputRef(timestamp, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, DATEDIFF, List.of(a, b));

        RexNode adapted = new DateDiffAdapter().adapt(original, List.of(), cluster);

        RexCall div = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.DIVIDE, div.getOperator());
        assertEquals(86400L, ((RexLiteral) div.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall diff = (RexCall) div.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MINUS, diff.getOperator());
        assertEquals("TO_UNIXTIME", ((RexCall) diff.getOperands().get(0)).getOperator().getName());
        assertEquals("TO_UNIXTIME", ((RexCall) diff.getOperands().get(1)).getOperator().getName());
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, DATEDIFF, List.of(rexBuilder.makeInputRef(bigint, 0)));

        expectThrows(IllegalArgumentException.class, () -> new DateDiffAdapter().adapt(original, List.of(), cluster));
    }
}
