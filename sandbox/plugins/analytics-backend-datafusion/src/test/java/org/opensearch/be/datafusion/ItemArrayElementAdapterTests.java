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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link ItemArrayElementAdapter}.
 *
 * <p>Asserts that array-typed {@code ITEM} calls rewrite to {@code array_element},
 * struct-typed pass through unchanged (genuine dispatch, not silent fallback),
 * and arity mismatches throw.
 */
public class ItemArrayElementAdapterTests extends OpenSearchTestCase {

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

    public void testArrayItemRewritesToArrayElement() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType arrayOfInt = typeFactory.createArrayType(integer, -1);
        RexNode arr = rexBuilder.makeInputRef(arrayOfInt, 0);
        RexNode idx = rexBuilder.makeLiteral(BigDecimal.ONE, integer, false);
        RexCall original = (RexCall) rexBuilder.makeCall(integer, SqlStdOperatorTable.ITEM, List.of(arr, idx));

        RexNode adapted = new ItemArrayElementAdapter().adapt(original, List.of(), cluster);

        RexCall out = (RexCall) adapted;
        assertEquals("array_element", out.getOperator().getName());
        assertEquals(2, out.getOperands().size());
    }

    public void testStructItemPassesThroughUnchanged() {
        // Struct ITEM: the first operand's Calcite type is a struct, not an array.
        // The adapter must NOT rewrite — DataFusion's substrait consumer handles it.
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType struct = typeFactory.builder().add("x", integer).build();
        RexNode s = rexBuilder.makeInputRef(struct, 0);
        RexNode fieldName = rexBuilder.makeLiteral("x");
        RexCall original = (RexCall) rexBuilder.makeCall(integer, SqlStdOperatorTable.ITEM, List.of(s, fieldName));

        RexNode adapted = new ItemArrayElementAdapter().adapt(original, List.of(), cluster);

        assertSame("struct ITEM passes through unchanged", original, adapted);
    }

    public void testWrongArityThrows() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType arrayOfInt = typeFactory.createArrayType(integer, -1);
        RexCall original = (RexCall) rexBuilder.makeCall(
            integer, SqlStdOperatorTable.ITEM, List.of(rexBuilder.makeInputRef(arrayOfInt, 0))
        );

        expectThrows(IllegalArgumentException.class, () -> new ItemArrayElementAdapter().adapt(original, List.of(), cluster));
    }
}
