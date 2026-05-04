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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link GetFormatAdapter}.
 *
 * <p>Asserts constant-fold to a format string literal, explicit throw on unknown
 * format, and explicit throw on arity mismatches. Non-literal args remain a
 * legitimate guard (returns the original call unchanged) — tested for regression.
 */
public class GetFormatAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction GET_FORMAT = new SqlFunction(
        "GET_FORMAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.STRING_STRING,
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

    private RexCall buildCall(String type, String region) {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        return (RexCall) rexBuilder.makeCall(
            varchar,
            GET_FORMAT,
            List.of(
                rexBuilder.makeLiteral(type, varchar, false),
                rexBuilder.makeLiteral(region, varchar, false)
            )
        );
    }

    public void testUsaDateFormat() {
        RexCall original = buildCall("date", "USA");

        RexNode adapted = new GetFormatAdapter().adapt(original, List.of(), cluster);

        assertTrue(adapted instanceof RexLiteral);
        assertEquals("%m.%d.%Y", ((RexLiteral) adapted).getValueAs(String.class));
    }

    public void testIsoTimestampFormat() {
        RexCall original = buildCall("timestamp", "iso");

        RexNode adapted = new GetFormatAdapter().adapt(original, List.of(), cluster);

        assertEquals("%Y-%m-%d %H:%i:%s", ((RexLiteral) adapted).getValueAs(String.class));
    }

    public void testUnknownFormatThrows() {
        RexCall original = buildCall("date", "MARS");

        expectThrows(IllegalArgumentException.class, () -> new GetFormatAdapter().adapt(original, List.of(), cluster));
    }

    public void testWrongArityThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexCall original = (RexCall) rexBuilder.makeCall(varchar, GET_FORMAT, List.of());

        expectThrows(IllegalArgumentException.class, () -> new GetFormatAdapter().adapt(original, List.of(), cluster));
    }

    public void testNonLiteralArgsPassThrough() {
        // Non-literal operands are a legitimate guard — the adapter returns the call
        // unchanged rather than throw, since the arg shape is surfaced elsewhere.
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode a = rexBuilder.makeInputRef(varchar, 0);
        RexNode b = rexBuilder.makeInputRef(varchar, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(varchar, GET_FORMAT, List.of(a, b));

        RexNode adapted = new GetFormatAdapter().adapt(original, List.of(), cluster);

        assertSame("non-literal args pass through unchanged", original, adapted);
    }
}
