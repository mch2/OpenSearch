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
 * Unit tests for {@link ToSecondsAdapter}.
 *
 * <p>Asserts {@code TO_SECONDS(ts) → to_unixtime(ts) + 62167219200} and that
 * arity mismatches throw.
 */
public class ToSecondsAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TO_SECONDS = new SqlFunction(
        "TO_SECONDS",
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

    public void testDecomposesToEpochPlusEraOffset() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexNode ts = rexBuilder.makeInputRef(timestamp, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TO_SECONDS, List.of(ts));

        RexNode adapted = new ToSecondsAdapter().adapt(original, List.of(), cluster);

        RexCall plus = (RexCall) adapted;
        assertEquals(SqlStdOperatorTable.PLUS, plus.getOperator());
        assertEquals(62167219200L, ((RexLiteral) plus.getOperands().get(1)).getValueAs(Long.class).longValue());
        assertEquals("TO_UNIXTIME", ((RexCall) plus.getOperands().get(0)).getOperator().getName());
    }

    public void testWrongArityThrows() {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RexCall original = (RexCall) rexBuilder.makeCall(bigint, TO_SECONDS, List.of());

        expectThrows(IllegalArgumentException.class, () -> new ToSecondsAdapter().adapt(original, List.of(), cluster));
    }
}
