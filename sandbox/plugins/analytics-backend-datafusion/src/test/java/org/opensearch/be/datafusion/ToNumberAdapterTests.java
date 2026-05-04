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
 * Unit tests for {@link ToNumberAdapter}.
 *
 * <p>Asserts {@code tonumber(x) → CAST(x AS DOUBLE)} and that arity mismatches throw.
 */
public class ToNumberAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TONUMBER = new SqlFunction(
        "TONUMBER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.NUMERIC
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    public void testRewritesToCastDouble() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType dbl = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexNode s = rexBuilder.makeInputRef(varchar, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(dbl, TONUMBER, List.of(s));

        RexNode adapted = new ToNumberAdapter().adapt(original, List.of(), cluster);

        assertEquals(SqlKind.CAST, adapted.getKind());
        assertEquals(SqlTypeName.DOUBLE, adapted.getType().getSqlTypeName());
    }

    public void testWrongArityThrows() {
        RelDataType dbl = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexCall original = (RexCall) rexBuilder.makeCall(dbl, TONUMBER, List.of());

        expectThrows(IllegalArgumentException.class, () -> new ToNumberAdapter().adapt(original, List.of(), cluster));
    }
}
