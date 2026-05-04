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
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link SymbolLiteralRewriter}.
 *
 * <p>Context: the PPL frontend's percentile_approx/median visitor emits a
 * metadata SYMBOL-typed {@code RexLiteral} alongside the aggregate's
 * value/percentage args. The convertor's {@link DataFusionFragmentConvertor#rewritePercentileApprox}
 * strips that SYMBOL arg from the {@code AggregateCall} and its immediate
 * input {@code Project}, but in fragment-split plans the SYMBOL literal
 * also rides in a separate, deeper {@code LogicalProject} that survives
 * the rewrite. Isthmus's {@code SubstraitRelVisitor} then chokes with
 * {@code Unable to handle symbol: &lt;TYPE&gt;} when visiting that Project.
 *
 * <p>The rewriter walks the entire {@link RelNode} tree and neutralizes
 * every {@code SqlTypeName.SYMBOL} {@link RexLiteral} by retyping it to a
 * typed-null INTEGER — column count is preserved so no downstream index
 * remapping is needed, and the substituted value is structurally dead
 * (the rewriter exists precisely because no downstream operator needs the
 * SYMBOL value).
 */
public class SymbolLiteralRewriterTests extends OpenSearchTestCase {

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

    /** A SYMBOL-typed RexLiteral — enum flag; isthmus cannot encode it. */
    private RexLiteral symbolLiteral() {
        // rexBuilder.makeFlag(...) produces a RexLiteral whose SqlTypeName is SYMBOL.
        return (RexLiteral) rexBuilder.makeFlag(SqlTypeName.BIGINT);
    }

    // ── Core behavior ───────────────────────────────────────────────────────

    /**
     * A {@link LogicalProject} that carries a SYMBOL {@link RexLiteral} alongside
     * real column references (the shape the PPL percentile_approx/median visitor
     * produces via {@code relBuilder.aggregateCall}). The rewriter must neutralize
     * the SYMBOL literal while preserving the Project's column count and other
     * expressions.
     */
    public void testSymbolLiteralInProjectIsRetyped() {
        RelNode scanNode = scan();
        RexNode aRef = rexBuilder.makeInputRef(scanNode, 0);

        // Project: [a, 50L, SYMBOL(BIGINT)] — mirrors the Project emitted upstream of a
        // percentile_approx aggregate, where the trailing column is the SYMBOL metadata arg.
        List<RexNode> exprs = List.of(
            aRef,
            rexBuilder.makeLiteral(50L, typeFactory.createSqlType(SqlTypeName.BIGINT)),
            symbolLiteral()
        );
        List<String> names = List.of("a", "pct", "sym");
        LogicalProject project = LogicalProject.create(scanNode, List.of(), exprs, names, java.util.Set.of());

        // Pre-condition: the Project does carry a SYMBOL-typed column.
        assertEquals("pre: SYMBOL literal present",
            SqlTypeName.SYMBOL, project.getProjects().get(2).getType().getSqlTypeName());

        RelNode rewritten = SymbolLiteralRewriter.rewrite(project);

        assertNoSymbolLiteralsAnywhere(rewritten);
        LogicalProject rewrittenProject = (LogicalProject) rewritten;
        assertEquals("column count preserved", 3, rewrittenProject.getProjects().size());
        // The pre-SYMBOL columns must be untouched.
        assertEquals(aRef, rewrittenProject.getProjects().get(0));
    }

    /** A deep tree — SYMBOL literals in a nested Project below a Filter below a Project
     *  must all be retyped. Mirrors the fragment-split plan where the SYMBOL rides
     *  in a Project several layers below the Aggregate. */
    public void testSymbolInDeeperProjectUnderOtherRels() {
        RelNode scanNode = scan();
        // Inner Project carries the SYMBOL literal.
        LogicalProject inner = LogicalProject.create(
            scanNode,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(scanNode, 0),
                symbolLiteral()
            ),
            List.of("a", "sym"),
            java.util.Set.of()
        );
        // Outer Project references inner's non-SYMBOL column only.
        LogicalProject outer = LogicalProject.create(
            inner,
            List.of(),
            List.of(rexBuilder.makeInputRef(inner, 0)),
            List.of("a"),
            java.util.Set.of()
        );

        RelNode rewritten = SymbolLiteralRewriter.rewrite(outer);

        assertNoSymbolLiteralsAnywhere(rewritten);
    }

    /** A Project with only typed literals must be returned unchanged. */
    public void testNoOpOnPlanWithoutSymbolLiterals() {
        RelNode scanNode = scan();
        LogicalProject project = LogicalProject.create(
            scanNode,
            List.of(),
            List.of(rexBuilder.makeInputRef(scanNode, 0)),
            List.of("a"),
            java.util.Set.of()
        );
        RelNode rewritten = SymbolLiteralRewriter.rewrite(project);
        assertEquals("project unchanged", project.getProjects(), ((LogicalProject) rewritten).getProjects());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /** Fails the test if any {@link RexLiteral} in any {@link LogicalProject} in the tree
     *  still has {@code SqlTypeName.SYMBOL}. The tests in this file only construct trees
     *  of Projects over scans, so Project coverage is sufficient. */
    private static void assertNoSymbolLiteralsAnywhere(RelNode root) {
        AtomicBoolean foundSymbol = new AtomicBoolean(false);
        List<String> offenders = new ArrayList<>();
        org.apache.calcite.rex.RexVisitorImpl<Void> symbolFinder = new org.apache.calcite.rex.RexVisitorImpl<>(true) {
            @Override
            public Void visitLiteral(RexLiteral literal) {
                if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
                    foundSymbol.set(true);
                    offenders.add(literal.toString());
                }
                return null;
            }
        };
        RelShuttle shuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalProject project) {
                for (RexNode expr : project.getProjects()) {
                    expr.accept(symbolFinder);
                }
                return super.visit(project);
            }
        };
        root.accept(shuttle);
        if (foundSymbol.get()) {
            fail("SYMBOL literal(s) remain after rewrite: " + offenders);
        }
    }
}
