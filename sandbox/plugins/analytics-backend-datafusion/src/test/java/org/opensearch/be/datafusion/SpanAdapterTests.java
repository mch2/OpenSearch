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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link SpanAdapter} — cat-3b adapter for PPL's numeric-
 * span path. Verifies the rewrite to the locally-declared {@code span}
 * operator AND the unit-operand retype that makes the substrait
 * serializer happy.
 */
public class SpanAdapterTests extends OpenSearchTestCase {

    /**
     * For numeric spans PPL's frontend emits {@code SPAN(field, n, NULL)}
     * where the null literal's Calcite type is {@link SqlTypeName#NULL}.
     * Isthmus's substrait serializer has no branch for Calcite NULL type
     * and throws {@code "Unable to convert the type NULL"} at fragment-
     * conversion time. The adapter must retype the unit operand to a
     * VARCHAR-nullable null literal, which isthmus CAN serialize.
     */
    public void testSpanRewritesAndRetypesUntypedNullUnitToVarcharNull() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        SqlFunction spanOp = new SqlFunction(
            "SPAN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE,
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode field = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode interval = rexBuilder.makeInputRef(intNullable, 1);
        // constantNull() produces a RexLiteral whose SqlTypeName is NULL.
        RexNode nullUnit = rexBuilder.constantNull();
        assertEquals("sanity: raw null unit has NULL type", SqlTypeName.NULL, nullUnit.getType().getSqlTypeName());
        RexCall original = (RexCall) rexBuilder.makeCall(spanOp, List.of(field, interval, nullUnit));

        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);

        assertTrue("adapted node must be a RexCall, got " + adapted.getClass(), adapted instanceof RexCall);
        RexCall call = (RexCall) adapted;
        assertSame("adapted call must target the locally-declared span operator", SpanAdapter.LOCAL_SPAN_OP, call.getOperator());
        assertEquals("span(field, interval, unit) must have 3 operands", 3, call.getOperands().size());
        assertSame("arg 0 must be the original field operand", field, call.getOperands().get(0));
        assertSame("arg 1 must be the original interval operand", interval, call.getOperands().get(1));
        // The unit operand is REPLACED (not the same instance as nullUnit) and
        // retyped to VARCHAR-nullable so isthmus's TypeConverter.toSubstrait
        // can serialize it.
        RexNode adaptedUnit = call.getOperands().get(2);
        assertTrue("arg 2 must still be a null literal (now VARCHAR-typed)", adaptedUnit instanceof RexLiteral);
        assertTrue("arg 2 literal must be null-valued", ((RexLiteral) adaptedUnit).isNull());
        assertEquals(
            "arg 2 must now be VARCHAR-typed (not NULL) so substrait can serialize",
            SqlTypeName.VARCHAR,
            adaptedUnit.getType().getSqlTypeName()
        );
    }

    /**
     * A non-null string unit (time-unit case that should be bridged coord-side
     * but might leak through to the adapter) must flow through unchanged.
     * Defensive — the Rust UDF returns a plan error if it sees a non-null
     * time unit, so this assertion guards against the adapter accidentally
     * masking that error by rewriting the non-null unit.
     */
    public void testNonNullUnitOperandPassesThroughUnchanged() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        SqlFunction spanOp = new SqlFunction(
            "SPAN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE,
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode field = rexBuilder.makeInputRef(doubleNullable, 0);
        RexNode interval = rexBuilder.makeInputRef(intNullable, 1);
        RexNode dayLit = rexBuilder.makeLiteral("d", varchar, true);
        RexCall original = (RexCall) rexBuilder.makeCall(spanOp, List.of(field, interval, dayLit));

        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);
        RexCall call = (RexCall) adapted;
        assertSame("non-null unit must pass through unchanged", dayLit, call.getOperands().get(2));
    }

    /**
     * The adapter MUST preserve the original call's {@link RelDataType}.
     * Regression guard in the same family as
     * {@link SpanBucketAdapterTests#testAdaptedCallPreservesOriginalReturnType}.
     */
    public void testAdaptedCallPreservesOriginalReturnType() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType intNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType doubleNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        SqlFunction spanOp = new SqlFunction(
            "SPAN",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(doubleNullable),
            null,
            OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RexNode field = rexBuilder.makeInputRef(intNullable, 0);
        RexNode interval = rexBuilder.makeInputRef(doubleNullable, 1);
        RexNode nullUnit = rexBuilder.constantNull();
        RexCall original = (RexCall) rexBuilder.makeCall(spanOp, List.of(field, interval, nullUnit));
        assertEquals(doubleNullable, original.getType());

        RexNode adapted = new SpanAdapter().adapt(original, List.of(), cluster);

        assertEquals("adapted call's return type must equal the original call's return type", original.getType(), adapted.getType());
    }
}
