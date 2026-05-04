/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchValues;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders a fully constant-folded Values-rooted plan to rows in-process, without
 * any shard dispatch or substrait round-trip.
 *
 * <p>When Calcite's {@code AGGREGATE_VALUES} + {@code PruneEmptyRules} collapse an
 * {@code Aggregate/Filter/Project/Sort} chain over a zero-row relation into a
 * literal projection over a single-row {@code OpenSearchValues} (e.g. the result
 * of {@code source=... | where 1=2 | stats sum(balance)} — see
 * {@code testSumEmpty}), the resulting plan contains no {@code OpenSearchTableScan}.
 * {@link DAGBuilder#build} would then fail because
 * {@code ShardTargetResolver} asserts a TableScan is present.
 *
 * <p>Instead, {@code DefaultPlanExecutor} checks
 * {@link #isScanLessCoordinatorFragment(RelNode)} and, if true, returns the result
 * of {@link #evaluate(RelNode)} directly to the caller's listener — bypassing the
 * DAG/scheduler entirely.
 *
 * <p>The evaluator intentionally supports only a narrow set of nodes — the exact
 * shapes the marking pipeline can produce. Anything else throws; this is a
 * guardrail against silently accepting plans the rule set should have already
 * constant-folded.
 *
 * @opensearch.internal
 */
public final class CoordinatorLocalFragmentEvaluator {

    private CoordinatorLocalFragmentEvaluator() {}

    /**
     * Returns true if {@code fragment} is the narrow "literal projection / sort
     * over a Values" shape that {@link #evaluate(RelNode)} can render. A
     * {@code false} return means the existing DAG/scheduler path must handle the
     * plan (e.g. it has a real shard scan).
     */
    public static boolean isScanLessCoordinatorFragment(RelNode fragment) {
        RelNode node = RelNodeUtils.unwrapHep(fragment);
        // The CBO inserts an OpenSearchExchangeReducer as the root SINGLETON enforcer.
        // A Values-rooted fragment still gets wrapped; strip it so the shape check
        // matches the post-DAGBuilder "child fragment" structure (Values / Project / Sort only).
        if (node instanceof OpenSearchExchangeReducer reducer) {
            node = RelNodeUtils.unwrapHep(reducer.getInput());
        }
        while (node instanceof OpenSearchSort || node instanceof OpenSearchProject) {
            // Project: require every projection to be a literal or input-ref.
            // If a projection is a RexCall, the plan isn't fully constant-folded
            // and must not be short-circuited.
            if (node instanceof OpenSearchProject project) {
                for (RexNode expr : project.getProjects()) {
                    if (!(expr instanceof RexLiteral) && !(expr instanceof RexInputRef)) {
                        return false;
                    }
                }
            }
            node = RelNodeUtils.unwrapHep(node.getInput(0));
        }
        return node instanceof OpenSearchValues;
    }

    /**
     * Evaluates a scan-less fragment to its constant result rows.
     *
     * @throws IllegalStateException when the fragment contains an unsupported node
     *     or non-literal expression — i.e. when {@link #isScanLessCoordinatorFragment}
     *     would have returned false, or when the fragment structure changed in a way
     *     we don't yet handle.
     */
    public static Iterable<Object[]> evaluate(RelNode fragment) {
        RelNode node = RelNodeUtils.unwrapHep(fragment);
        // Strip the CBO-inserted SINGLETON exchange wrapper. See isScanLessCoordinatorFragment.
        if (node instanceof OpenSearchExchangeReducer reducer) {
            node = RelNodeUtils.unwrapHep(reducer.getInput());
        }

        // Skip Sort — over a tiny constant relation, offset/fetch are no-ops
        // (task #9 guarantees tuples=[[{...}]] i.e. exactly one row, and the
        // default fetch of 10000 trivially holds).
        while (node instanceof OpenSearchSort sort) {
            node = RelNodeUtils.unwrapHep(sort.getInput());
        }

        if (node instanceof OpenSearchProject project) {
            RelNode child = RelNodeUtils.unwrapHep(project.getInput());
            if (!(child instanceof OpenSearchValues values)) {
                throw new IllegalStateException(
                    "Coordinator-local fragment: Project child must be OpenSearchValues, got " + child.getClass().getSimpleName()
                );
            }
            return projectRows(project, values);
        }

        if (node instanceof OpenSearchValues values) {
            return valuesRows(values);
        }

        throw new IllegalStateException(
            "Coordinator-local fragment: only literal-projection over Values supported; got " + node.getClass().getSimpleName()
        );
    }

    // ── Internal ────────────────────────────────────────────────────────────

    private static Iterable<Object[]> projectRows(OpenSearchProject project, OpenSearchValues values) {
        List<Object[]> inputRows = new ArrayList<>();
        for (Object[] row : valuesRows(values)) {
            inputRows.add(row);
        }
        List<Object[]> out = new ArrayList<>(inputRows.size());
        for (Object[] inputRow : inputRows) {
            Object[] outRow = new Object[project.getProjects().size()];
            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode expr = project.getProjects().get(i);
                outRow[i] = evaluateRex(expr, inputRow);
            }
            out.add(outRow);
        }
        return out;
    }

    private static Iterable<Object[]> valuesRows(OpenSearchValues values) {
        // Values tuples are typed as ImmutableList<ImmutableList<RexLiteral>>, so every
        // entry is a RexLiteral by construction. No defensive instanceof check needed.
        List<Object[]> rows = new ArrayList<>(values.getTuples().size());
        for (List<RexLiteral> tuple : values.getTuples()) {
            Object[] row = new Object[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                row[i] = literalValue(tuple.get(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private static Object evaluateRex(RexNode expr, Object[] inputRow) {
        if (expr instanceof RexLiteral literal) {
            return literalValue(literal);
        }
        if (expr instanceof RexInputRef ref) {
            return inputRow[ref.getIndex()];
        }
        throw new IllegalStateException(
            "Coordinator-local fragment: expected constant-folded literal/input-ref, got " + expr.getClass().getSimpleName()
        );
    }

    /** Returns the Java-typed value of a RexLiteral, or null. */
    private static Object literalValue(RexLiteral literal) {
        if (literal.isNull()) return null;
        // getValue3 returns the "typed" form Calcite uses for literals — e.g.
        // BigDecimal for numerics, String for VARCHAR, Boolean for BOOLEAN.
        // For the testSumEmpty path we only ever hit isNull(); getValue3 is
        // the sensible fallback for any non-null literal we may encounter.
        return literal.getValue3();
    }
}
