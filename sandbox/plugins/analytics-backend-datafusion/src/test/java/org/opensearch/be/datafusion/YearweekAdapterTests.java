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
 * Unit tests for {@link YearweekAdapter}.
 *
 * <p>Asserts the adapter's decomposition shape
 * ({@code YEARWEEK(ts) → date_part('year', ts)*100 + date_part('week', ts)}) and
 * verifies that invariant #5 (no silent fallbacks) holds: zero operands must
 * throw, not return the original call unchanged.
 */
public class YearweekAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction YEARWEEK = new SqlFunction(
        "YEARWEEK",
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

    public void testDecomposesToYearTimesHundredPlusWeek() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, YEARWEEK, List.of(ts));

        RexNode adapted = new YearweekAdapter().adapt(original, List.of(), cluster);

        // Outermost op: PLUS(year*100, week)
        RexCall outer = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, outer.getOperator());
        RexCall yearTimes100 = (RexCall) outer.getOperands().get(0);
        assertEquals(SqlStdOperatorTable.MULTIPLY, yearTimes100.getOperator());
        RexCall yearPart = (RexCall) yearTimes100.getOperands().get(0);
        assertEquals("DATE_PART", yearPart.getOperator().getName());
        assertEquals("year", ((RexLiteral) yearPart.getOperands().get(0)).getValueAs(String.class));
        assertEquals(100L, ((RexLiteral) yearTimes100.getOperands().get(1)).getValueAs(Long.class).longValue());
        RexCall weekPart = (RexCall) outer.getOperands().get(1);
        assertEquals("DATE_PART", weekPart.getOperator().getName());
        assertEquals("week", ((RexLiteral) weekPart.getOperands().get(0)).getValueAs(String.class));
    }

    public void testEmptyOperandsThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, YEARWEEK, List.of());

        expectThrows(IllegalArgumentException.class, () -> new YearweekAdapter().adapt(original, List.of(), cluster));
    }
}
