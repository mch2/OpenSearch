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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link ArrayCompactAdapter}.
 *
 * <p>Asserts {@code array_compact(list<T>) → array_remove_all(list<T>, CAST(NULL AS T))}
 * for string and integer element types, and that arity mismatches throw.
 */
public class ArrayCompactAdapterTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    public void testRewritesStringArrayToArrayRemoveAll() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType arrayOfVarchar = typeFactory.createArrayType(varchar, -1);
        RexNode arr = rexBuilder.makeInputRef(arrayOfVarchar, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(arrayOfVarchar, SqlLibraryOperators.ARRAY_COMPACT, List.of(arr));

        RexNode adapted = new ArrayCompactAdapter().adapt(original, List.of(), cluster);

        RexCall out = (RexCall) adapted;
        assertEquals("array_remove_all", out.getOperator().getName());
        assertEquals(2, out.getOperands().size());
        assertSame("first operand passes through unchanged", arr, out.getOperands().get(0));
        RexNode second = out.getOperands().get(1);
        assertTrue("second operand must be a NULL literal", RexLiteral.isNullLiteral(second));
        assertEquals(SqlTypeName.VARCHAR, second.getType().getSqlTypeName());
    }

    public void testRewritesIntegerArrayToArrayRemoveAll() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType arrayOfInt = typeFactory.createArrayType(integer, -1);
        RexNode arr = rexBuilder.makeInputRef(arrayOfInt, 0);
        RexCall original = (RexCall) rexBuilder.makeCall(arrayOfInt, SqlLibraryOperators.ARRAY_COMPACT, List.of(arr));

        RexNode adapted = new ArrayCompactAdapter().adapt(original, List.of(), cluster);

        RexCall out = (RexCall) adapted;
        assertEquals("array_remove_all", out.getOperator().getName());
        assertEquals(2, out.getOperands().size());
        RexNode second = out.getOperands().get(1);
        assertTrue("second operand must be a NULL literal", RexLiteral.isNullLiteral(second));
        assertEquals(SqlTypeName.INTEGER, second.getType().getSqlTypeName());
    }

    public void testWrongArityThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType arrayOfVarchar = typeFactory.createArrayType(varchar, -1);
        RexNode arr = rexBuilder.makeInputRef(arrayOfVarchar, 0);
        RexNode extra = rexBuilder.makeInputRef(varchar, 1);
        RexCall original = (RexCall) rexBuilder.makeCall(
            arrayOfVarchar, SqlLibraryOperators.ARRAY_COMPACT, List.of(arr, extra)
        );

        expectThrows(IllegalArgumentException.class, () -> new ArrayCompactAdapter().adapt(original, List.of(), cluster));
    }

    public void testNonArrayOperandThrows() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode notArray = rexBuilder.makeInputRef(varchar, 0);
        // Construct directly — SqlLibraryOperators.ARRAY_COMPACT's operand checker would normally
        // reject this, but the adapter must also validate at rewrite time.
        RexCall original = (RexCall) rexBuilder.makeCall(
            typeFactory.createArrayType(varchar, -1), SqlLibraryOperators.ARRAY_COMPACT, List.of(notArray)
        );

        expectThrows(IllegalArgumentException.class, () -> new ArrayCompactAdapter().adapt(original, List.of(), cluster));
    }
}
