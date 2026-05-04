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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link TimeDiffAdapter}.
 *
 * <p>Asserts {@code TIMEDIFF(a, b) → make_time(d/3600, (d%3600)/60, d%60)} over
 * {@code d = to_unixtime(a) - to_unixtime(b)}; arity mismatches throw.
 */
public class TimeDiffAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TIMEDIFF = new SqlFunction(
        "TIMEDIFF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME,
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

    public void testDecomposesToMakeTimeOverEpochDelta() {
        RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode a = rexBuilder.makeInputRef(timestamp, 0);
        RexNode b = rexBuilder.makeInputRef(timestamp, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(time, TIMEDIFF, List.of(a, b));

        RexNode adapted = new TimeDiffAdapter().adapt(original, List.of(), cluster);

        RexCall outer = (RexCall) adapted;
        assertEquals("MAKE_TIME", outer.getOperator().getName());
        assertEquals(3, outer.getOperands().size());
    }

    public void testWrongArityThrows() {
        RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
        RexNode a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), 0);
        RexCall original = (RexCall) rexBuilder.makeCall(time, TIMEDIFF, List.of(a));

        expectThrows(IllegalArgumentException.class, () -> new TimeDiffAdapter().adapt(original, List.of(), cluster));
    }
}
