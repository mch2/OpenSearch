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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link UntypedNullRewriter}.
 *
 * <p>The rewriter replaces operand-position untyped null literals (SqlTypeName.NULL)
 * with typed nulls matching the enclosing {@link RexCall}'s result type. The most
 * common source is the PPL trendline visitor's {@code CASE(cond, then, null)} where
 * the ELSE branch uses {@code context.relBuilder.literal(null)}, which Calcite
 * types as NULL.
 */
public class UntypedNullRewriterTests extends OpenSearchTestCase {

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

    private RelDataType rowType() {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("a", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true));
        return b.build();
    }

    private RelNode scan() {
        return new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), "t", rowType());
    }

    /** Literal that the Calcite relBuilder produces for {@code literal(null)} — typed as NULL. */
    private RexLiteral untypedNull() {
        return rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
    }

    // ── Core behavior ───────────────────────────────────────────────────────

    /**
     * {@code CASE(a > 0, 1.0, null:NULL)} — the ELSE branch is untyped; the CASE
     * itself is DOUBLE (from the THEN branch's DECIMAL → DOUBLE widening). The
     * rewriter must replace the ELSE with a typed null whose type matches the
     * enclosing CASE.
     */
    public void testCaseWithUntypedNullElseBranchIsRetyped() {
        RelNode scanNode = scan();
        RexNode aRef = rexBuilder.makeInputRef(scanNode, 0);

        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            aRef,
            rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
        RexNode thenBranch = rexBuilder.makeLiteral(1.0d, typeFactory.createSqlType(SqlTypeName.DOUBLE));
        RexCall caseExpr = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CASE, cond, thenBranch, untypedNull());
        assertEquals("CASE return type is DOUBLE", SqlTypeName.DOUBLE, caseExpr.getType().getSqlTypeName());

        RelNode project = LogicalProject.create(scanNode, List.of(), List.of(caseExpr), List.of("out"), java.util.Set.of());
        RelNode rewritten = UntypedNullRewriter.rewrite(project);

        RexCall newCase = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);
        RexNode newElse = newCase.getOperands().get(2);
        assertTrue("ELSE must still be a literal null", newElse instanceof RexLiteral && ((RexLiteral) newElse).isNull());
        assertEquals("ELSE must be typed as the enclosing call's result type",
            SqlTypeName.DOUBLE, newElse.getType().getSqlTypeName());
    }

    /**
     * A nested {@code +} call inside an outer CASE — the rewriter must descend
     * into the THEN branch and apply the fix to any untyped nulls found deep
     * in the tree.
     */
    public void testNestedUntypedNullIsRetyped() {
        RelNode scanNode = scan();
        RexNode aRef = rexBuilder.makeInputRef(scanNode, 0);

        // CASE(a > 0, a + NULL_untyped, 0)
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            aRef,
            rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
        RexCall addCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS, aRef, untypedNull());
        RexNode elseBranch = rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT));
        RexCall caseExpr = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CASE, cond, addCall, elseBranch);

        RelNode project = LogicalProject.create(scanNode, List.of(), List.of(caseExpr), List.of("out"), java.util.Set.of());
        RelNode rewritten = UntypedNullRewriter.rewrite(project);

        RexCall newCase = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);
        RexCall newAdd = (RexCall) newCase.getOperands().get(1);
        RexNode nullOperand = newAdd.getOperands().get(1);
        assertTrue("inner null must remain a literal null",
            nullOperand instanceof RexLiteral && ((RexLiteral) nullOperand).isNull());
        assertEquals("inner null must inherit the enclosing PLUS call's result type (BIGINT)",
            SqlTypeName.BIGINT, nullOperand.getType().getSqlTypeName());
    }

    /** Literals that are already typed must be left alone — idempotent on valid plans. */
    public void testTypedNullIsNotRewritten() {
        RelNode scanNode = scan();
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexNode typedNull = rexBuilder.makeNullLiteral(doubleType);

        RexNode cond = rexBuilder.makeLiteral(true);
        RexNode thenBranch = rexBuilder.makeLiteral(1.0d, doubleType);
        RexCall caseExpr = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CASE, cond, thenBranch, typedNull);

        RelNode project = LogicalProject.create(scanNode, List.of(), List.of(caseExpr), List.of("out"), java.util.Set.of());
        RelNode rewritten = UntypedNullRewriter.rewrite(project);

        RexCall newCase = (RexCall) ((LogicalProject) rewritten).getProjects().get(0);
        RexNode newElse = newCase.getOperands().get(2);
        assertEquals("already-typed null remains DOUBLE", SqlTypeName.DOUBLE, newElse.getType().getSqlTypeName());
    }

    /** A filter condition carrying an untyped-null operand must also be fixed. */
    public void testFilterConditionIsRewritten() {
        RelNode scanNode = scan();
        RexNode aRef = rexBuilder.makeInputRef(scanNode, 0);
        // COALESCE(a, NULL_untyped) > 0 — COALESCE's return type is BIGINT (from aRef).
        RexCall coalesce = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, aRef, untypedNull());
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            coalesce,
            rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
        RelNode filter = LogicalFilter.create(scanNode, cond);
        RelNode rewritten = UntypedNullRewriter.rewrite(filter);

        RexCall rewrittenCond = (RexCall) ((LogicalFilter) rewritten).getCondition();
        RexCall rewrittenCoalesce = (RexCall) rewrittenCond.getOperands().get(0);
        RexNode rewrittenNull = rewrittenCoalesce.getOperands().get(1);
        assertEquals("nested null in filter condition must be retyped to match enclosing COALESCE",
            SqlTypeName.BIGINT, rewrittenNull.getType().getSqlTypeName());
    }

    /** A plan with no untyped nulls must be returned unchanged in structure. */
    public void testNoOpOnPlanWithoutUntypedNulls() {
        RelNode scanNode = scan();
        RexNode aRef = rexBuilder.makeInputRef(scanNode, 0);
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            aRef,
            rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
        RelNode filter = LogicalFilter.create(scanNode, cond);
        RelNode rewritten = UntypedNullRewriter.rewrite(filter);
        assertEquals("condition unchanged", cond, ((LogicalFilter) rewritten).getCondition());
    }

    @SuppressWarnings("unused")
    private static void keepUnusedImport(OpenSearchStageInputScan ignored) {
        // Keep the import alive; StageInputTableScan extends a similar base and having this import
        // documented in the test file matches the rest of the test-suite style.
    }
}
