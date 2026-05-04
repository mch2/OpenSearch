/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Pre-substrait rewrite that neutralizes operand-position SYMBOL
 * {@link RexLiteral} nodes anywhere in a {@link RelNode} tree.
 *
 * <p>Context: the PPL frontend's {@code percentile_approx} / {@code median}
 * visitor emits a metadata SYMBOL-typed {@code RexLiteral}
 * ({@code rexBuilder.makeFlag(field.getType().getSqlTypeName())}) alongside
 * the aggregate's value and percent args. Calcite's {@code relBuilder.aggregateCall}
 * projects each argument into a new {@link Project} above the aggregate's input —
 * that Project carries the SYMBOL literal as one of its columns. The
 * {@link DataFusionFragmentConvertor#rewritePercentileApprox} pass strips the
 * SYMBOL from the {@code AggregateCall} and skips it when building the
 * immediate input Project, but in fragment-split plans the SYMBOL literal
 * also rides in a separate, deeper Project that survives the rewrite.
 * Isthmus's {@code SubstraitRelVisitor} / {@code LiteralConverter} then hits
 * that Project during substrait encoding and throws
 * {@code "Unable to handle symbol: &lt;TYPE&gt;"}.
 *
 * <p>Semantic note: replacing a SYMBOL literal with a typed-null INTEGER is
 * safe because the SYMBOL arg is structurally dead by the time this rewriter
 * runs — the {@code rewritePercentileApprox} pass has already severed every
 * AggregateCall's reference to it. The literal still occupies a column slot
 * in the Project (so indices stay aligned for any RexInputRef above) but
 * nothing consumes its value.
 *
 * @opensearch.internal
 */
final class SymbolLiteralRewriter {

    private SymbolLiteralRewriter() {}

    /** Rewrites every expression reachable from {@code root}, returning a new
     *  tree if any SYMBOL literal was retyped. */
    static RelNode rewrite(RelNode root) {
        return root.accept(new Shuttle());
    }

    /** RelShuttle that applies {@link Fixer} to every expression-bearing RelNode. */
    private static final class Shuttle extends RelShuttleImpl {
        @Override
        public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return applyFix(visited);
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalProject project) {
            return applyFix(super.visit(project));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalFilter filter) {
            return applyFix(super.visit(filter));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalJoin join) {
            return applyFix(super.visit(join));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalAggregate aggregate) {
            return applyFix(super.visit(aggregate));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalSort sort) {
            return applyFix(super.visit(sort));
        }

        private RelNode applyFix(RelNode node) {
            if (node instanceof Project || node instanceof Filter || node instanceof Join
                || node instanceof Aggregate || node instanceof Sort) {
                RexBuilder rexBuilder = node.getCluster().getRexBuilder();
                return node.accept(new Fixer(rexBuilder));
            }
            return node;
        }
    }

    /** RexShuttle that replaces every {@link RexLiteral} whose type is
     *  {@code SqlTypeName.SYMBOL} with a typed-null INTEGER literal. */
    private static final class Fixer extends RexShuttle {
        private final RexBuilder rexBuilder;

        Fixer(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
                return rexBuilder.makeNullLiteral(
                    rexBuilder.getTypeFactory().createTypeWithNullability(
                        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
            }
            return literal;
        }
    }
}
