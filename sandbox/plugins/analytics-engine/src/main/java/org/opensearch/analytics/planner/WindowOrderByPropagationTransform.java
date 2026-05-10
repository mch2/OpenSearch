/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Pre-CBO transform that propagates an upstream {@link OpenSearchSort}'s collation into a
 * windowed {@link OpenSearchProject}'s {@link RexOver} that has empty {@code ORDER BY}.
 *
 * <p>Without this, DataFusion's window operator processes input in unspecified order,
 * breaking running aggregates over a sorted stream (e.g. PPL {@code | sort key |
 * streamstats count() as running}). Globally-framed windows (both bounds {@code UNBOUNDED})
 * are skipped — adding an {@code ORDER BY} there would narrow the frame to {@code RANGE
 * UNBOUNDED PRECEDING TO CURRENT ROW} (running), changing the semantics of eventstats-style
 * broadcast aggregates.
 *
 * <p>This is a separate concern from gather placement (handled by
 * {@link org.opensearch.analytics.planner.rules.OpenSearchWindowedProjectGatherRule} during
 * Volcano CBO). The two used to share a class; the split makes each responsibility
 * independently reasonable about and testable.
 *
 * @opensearch.internal
 */
final class WindowOrderByPropagationTransform {

    private WindowOrderByPropagationTransform() {}

    static RelNode apply(RelNode root) {
        return walk(root);
    }

    private static RelNode walk(RelNode node) {
        List<RelNode> rewrittenInputs = new ArrayList<>(node.getInputs().size());
        boolean anyChanged = false;
        for (RelNode child : node.getInputs()) {
            RelNode rewrittenChild = walk(child);
            anyChanged |= rewrittenChild != child;
            rewrittenInputs.add(rewrittenChild);
        }
        RelNode current = anyChanged ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
        if (current instanceof OpenSearchProject project && isWindowedProject(project)) {
            return propagateSortIntoWindow(project);
        }
        return current;
    }

    /**
     * If the input chain has an immediate Sort whose collation we can attach to a RexOver
     * with empty ORDER BY, rebuild the project's expressions with an OVER that explicitly
     * orders by the sort's fields. Returns the original project unchanged when there's no
     * sort to propagate, no RexOver with empty ORDER BY, or any other rewrite obstacle.
     */
    private static RelNode propagateSortIntoWindow(OpenSearchProject project) {
        OpenSearchSort sort = findUpstreamSort(project.getInput());
        if (sort == null) return project;
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        ImmutableList<RexFieldCollation> orderKeys = sortCollationToOrderKeys(sort.getCollation(), rexBuilder, project.getInput());
        if (orderKeys.isEmpty()) return project;
        List<RexNode> rewritten = new ArrayList<>(project.getProjects().size());
        boolean anyChanged = false;
        for (RexNode expr : project.getProjects()) {
            RexNode result = rewriteRexOverOrderKeys(expr, orderKeys, rexBuilder);
            anyChanged |= result != expr;
            rewritten.add(result);
        }
        if (!anyChanged) return project;
        return project.copy(project.getTraitSet(), project.getInput(), rewritten, project.getRowType());
    }

    /** Walks down through Sort/Filter/Project/ExchangeReducer layers, returns the first OpenSearchSort or null. */
    private static OpenSearchSort findUpstreamSort(RelNode node) {
        if (node instanceof OpenSearchSort sort) return sort;
        if (node instanceof OpenSearchFilter || node instanceof OpenSearchProject || node instanceof OpenSearchExchangeReducer) {
            return findUpstreamSort(node.getInputs().getFirst());
        }
        return null;
    }

    private static ImmutableList<RexFieldCollation> sortCollationToOrderKeys(RelCollation collation, RexBuilder rexBuilder, RelNode input) {
        ImmutableList.Builder<RexFieldCollation> builder = ImmutableList.builder();
        for (RelFieldCollation fc : collation.getFieldCollations()) {
            int idx = fc.getFieldIndex();
            if (idx >= input.getRowType().getFieldCount()) return ImmutableList.of();
            RexNode ref = rexBuilder.makeInputRef(input.getRowType().getFieldList().get(idx).getType(), idx);
            Set<SqlKind> direction = EnumSet.noneOf(SqlKind.class);
            if (fc.direction == RelFieldCollation.Direction.DESCENDING) direction.add(SqlKind.DESCENDING);
            if (fc.nullDirection == RelFieldCollation.NullDirection.FIRST) direction.add(SqlKind.NULLS_FIRST);
            else if (fc.nullDirection == RelFieldCollation.NullDirection.LAST) direction.add(SqlKind.NULLS_LAST);
            builder.add(new RexFieldCollation(ref, direction));
        }
        return builder.build();
    }

    /**
     * If the expression is (or wraps) a RexOver with empty orderKeys, rebuild it with the
     * provided orderKeys. Annotated wrappers are unwrapped, rewritten, and re-wrapped.
     */
    private static RexNode rewriteRexOverOrderKeys(RexNode expr, ImmutableList<RexFieldCollation> orderKeys, RexBuilder rexBuilder) {
        if (expr instanceof AnnotatedProjectExpression annotated && annotated.getOriginal() instanceof RexOver over) {
            RexNode rebuilt = rebuildRexOverWithOrderKeys(over, orderKeys, rexBuilder);
            return rebuilt == over ? expr : annotated.withAdaptedOriginal(rebuilt);
        }
        if (expr instanceof RexOver over) return rebuildRexOverWithOrderKeys(over, orderKeys, rexBuilder);
        return expr;
    }

    private static RexNode rebuildRexOverWithOrderKeys(RexOver over, ImmutableList<RexFieldCollation> orderKeys, RexBuilder rexBuilder) {
        if (!over.getWindow().orderKeys.isEmpty()) return over;
        // Skip globally-framed windows (both bounds UNBOUNDED) — adding ORDER BY there would
        // narrow the frame to RANGE UNBOUNDED PRECEDING TO CURRENT ROW (running), changing the
        // semantics of eventstats-style broadcast aggregates.
        if (over.getWindow().getLowerBound().isUnbounded() && over.getWindow().getUpperBound().isUnbounded()) {
            return over;
        }
        return rexBuilder.makeOver(
            over.getType(),
            over.getAggOperator(),
            over.getOperands(),
            over.getWindow().partitionKeys,
            orderKeys,
            over.getWindow().getLowerBound(),
            over.getWindow().getUpperBound(),
            over.getWindow().getExclude(),
            over.getWindow().isRows(),
            true,
            false,
            over.isDistinct(),
            over.ignoreNulls()
        );
    }

    private static boolean isWindowedProject(OpenSearchProject project) {
        OverDetector detector = new OverDetector();
        for (RexNode expr : project.getProjects()) {
            expr.accept(detector);
            if (detector.found) return true;
        }
        return false;
    }

    private static final class OverDetector extends RexVisitorImpl<Void> {
        boolean found;

        OverDetector() {
            super(true);
        }

        @Override
        public Void visitOver(RexOver over) {
            found = true;
            return null;
        }
    }
}
