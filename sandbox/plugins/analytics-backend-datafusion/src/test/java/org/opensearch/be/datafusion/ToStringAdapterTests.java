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
 * Unit tests for {@link ToStringAdapter}.
 *
 * <p>Asserts {@code tostring(x) → CAST(x AS VARCHAR)} and that arity mismatches throw.
 */
public class ToStringAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    private static final SqlFunction TOSTRING = new SqlFunction(
        "TOSTRING",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.ANY,
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

    public void testRewritesToCastVarchar() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode n = rexBuilder.makeInputRef(integer, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(varchar, TOSTRING, List.of(n));

        RexNode adapted = new ToStringAdapter().adapt(original, List.of(), cluster);

        assertEquals(SqlKind.CAST, adapted.getKind());
        assertEquals(SqlTypeName.VARCHAR, adapted.getType().getSqlTypeName());
    }

    public void testWrongArityThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexCall original = (RexCall) rexBuilder.makeCall(varchar, TOSTRING, List.of());

        expectThrows(IllegalArgumentException.class, () -> new ToStringAdapter().adapt(original, List.of(), cluster));
    }
}
