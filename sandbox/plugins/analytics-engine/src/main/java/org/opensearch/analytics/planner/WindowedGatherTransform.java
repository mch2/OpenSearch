/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-CBO AST transform that lowers the SINGLETON exchange position for windowed-Project
 * subtrees. A {@link OpenSearchProject} containing a {@link RexOver} computes a window
 * aggregate over a totally-ordered global stream — running sums, ranks, frame-bounded
 * aggregates, etc. Executing it over partitioned input is incorrect: each shard would produce
 * its own per-shard window state and the coordinator gather would concatenate mismatched
 * results.
 *
 * <p>Volcano without top-down optimization cannot push a SINGLETON requirement through
 * already-marked operators (the parent operator was created during Hep marking with its child's
 * traits and Volcano won't auto-explore alternative trait combinations on the parent's
 * subset). Top-down optimization is invasive — it requires {@code Convention.enforce} and
 * {@link org.apache.calcite.rel.PhysicalNode#deriveTraits} on every operator, and would
 * conflict with {@code OpenSearchAggregateSplitRule}'s bottom-up trait-conversion pattern.
 *
 * <p>Instead, this transform runs between the Hep marking phase and Volcano CBO. It walks the
 * marked tree bottom-up, finds every windowed Project, identifies the windowed-affected
 * subtree (the windowed Project plus all distribution-preserving ancestors and descendants),
 * inserts an {@link OpenSearchExchangeReducer} at the deepest boundary in the chain, and
 * rewrites every operator in the affected subtree with SINGLETON traits. Volcano then sees a
 * pre-cooked SINGLETON-distributed subtree and optimizes around it without trait conversion
 * battles.
 *
 * <h2>Scope</h2>
 *
 * <p>Distribution-preserving operators recognized today: {@link OpenSearchSort},
 * {@link OpenSearchProject}, {@link OpenSearchFilter}. These are walked through and rewritten
 * with SINGLETON traits when in the windowed subtree. Anything else (Scan, Aggregate, Union,
 * future Correlate) is treated as a boundary — the gather sits above it.
 *
 * <p>Non-windowed query plans pass through this transform unchanged: the bottom-up walk
 * detects no {@link RexOver} and returns the original tree structure.
 *
 * <h2>Future extension to Correlate</h2>
 *
 * <p>When PR 21480 lands and we enable Correlate-based streamstats by-group execution,
 * Correlate is added to the windowed-detector — its left input distribution flows through the
 * Correlate, so the same propagation logic applies. The right side is per-row (input
 * distribution does not constrain it) and is handled by the existing multi-input cut machinery
 * in {@code MultiInputShape}.
 *
 * <h2>Boundaries below the affected chain</h2>
 *
 * <p>If the chain reaches {@code OpenSearchAggregate}, the gather is inserted above the
 * Aggregate. The Aggregate is then split by {@code OpenSearchAggregateSplitRule} during CBO
 * into PARTIAL+Reducer+FINAL — producing a redundant inner reducer. This is correct but
 * suboptimal; refining to skip the outer reducer when an Aggregate split-reducer already
 * provides SINGLETON output is left as a follow-up.
 *
 * @opensearch.internal
 */
final class WindowedGatherTransform {

    private WindowedGatherTransform() {}

    /**
     * Apply the transform. Returns the input unchanged when no windowed Project is present
     * — non-windowed query plans pay the cost of one bottom-up tree walk and nothing else.
     */
    static RelNode apply(RelNode root, OpenSearchDistributionTraitDef distTraitDef) {
        return walk(root, distTraitDef).node;
    }

    /**
     * Bottom-up rewrite. Returns the (possibly-rebuilt) node and a flag indicating whether the
     * subtree rooted here is windowed-affected — i.e., contains a windowed Project below or
     * is itself the windowed Project. Affected subtrees propagate up through distribution-
     * preserving ancestors, who get rewritten with SINGLETON traits as the propagation
     * climbs.
     */
    private static Result walk(RelNode node, OpenSearchDistributionTraitDef distTraitDef) {
        List<RelNode> rewrittenInputs = new ArrayList<>(node.getInputs().size());
        boolean anyChildAffected = false;
        for (RelNode child : node.getInputs()) {
            Result childResult = walk(child, distTraitDef);
            rewrittenInputs.add(childResult.node);
            anyChildAffected |= childResult.windowedAffected;
        }

        if (isWindowedProject(node)) {
            // The windowed Project is the boundary at which we SWITCH from "any distribution
            // is fine" (below) to "SINGLETON required" (here and above). Rewrite the input
            // chain: insert an exchange reducer at the deepest preservable position and
            // bring every operator above it up to SINGLETON traits.
            RelNode windowedInput = node.getInputs().getFirst();
            RelNode rewrittenInput = lowerGatherInChain(windowedInput, distTraitDef);
            RelNode rewrittenNode = node.copy(
                node.getTraitSet().replace(distTraitDef.singleton()),
                List.of(rewrittenInput)
            );
            return new Result(rewrittenNode, true);
        }

        if (anyChildAffected) {
            // A windowed Project is somewhere below. If this node is distribution-preserving,
            // propagate the SINGLETON traits up — operators above the windowed Project always
            // need to run on coordinator because they consume already-windowed output that
            // only exists post-gather. Otherwise terminate propagation here.
            if (propagatesSingletonUpward(node)) {
                RelNode rewritten = node.copy(node.getTraitSet().replace(distTraitDef.singleton()), rewrittenInputs);
                return new Result(rewritten, true);
            }
            RelNode rewritten = inputsChanged(node, rewrittenInputs) ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
            return new Result(rewritten, false);
        }

        RelNode rewritten = inputsChanged(node, rewrittenInputs) ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
        return new Result(rewritten, false);
    }

    /**
     * Walk down through operators that <em>must</em> execute over a SINGLETON-gathered stream
     * (i.e. the gather must be inserted below them). The first operator that <em>doesn't</em>
     * require a gathered input becomes the gather's location: the gather is inserted above it
     * so the operator stays on the data node and runs per-shard.
     *
     * <p>Today only {@link OpenSearchSort} requires gathered input — global ordering and
     * top-K need a SINGLETON stream, otherwise per-shard sort + concat ≠ global sort. Filters
     * and column-projections are row-local; running them per-shard before the gather is both
     * correct and cheaper (the filter drops rows that would otherwise be shipped, and a
     * narrowing project shrinks the per-row payload through the exchange). Treating them as
     * boundaries here is the difference between &quot;ship everything to coordinator and
     * filter/project there&quot; and &quot;filter/project per-shard then ship the survivors.&quot;
     */
    private static RelNode lowerGatherInChain(RelNode node, OpenSearchDistributionTraitDef distTraitDef) {
        if (requiresGatheredInput(node)) {
            RelNode innerRewritten = lowerGatherInChain(node.getInputs().getFirst(), distTraitDef);
            return node.copy(node.getTraitSet().replace(distTraitDef.singleton()), List.of(innerRewritten));
        }
        // Boundary reached. If the boundary is already SINGLETON-distributed (e.g. a single-shard
        // table scan or an Aggregate that has been split into PARTIAL+Reducer+FINAL), no gather
        // is needed — adding one would force the data-node fragment to become a bare scan over
        // the full table schema, which is wasteful and exposes column types the fragment
        // converter may not handle yet (TIMESTAMP, etc., that the upstream Project would
        // otherwise have projected away). Returning the node unchanged keeps the existing
        // single-shard plan intact.
        if (alreadySingleton(node)) {
            return node;
        }
        return makeGather(node, distTraitDef);
    }

    private static RelNode makeGather(RelNode input, OpenSearchDistributionTraitDef distTraitDef) {
        if (!(input instanceof OpenSearchRelNode openSearchInput)) {
            throw new IllegalStateException(
                "WindowedGatherTransform expected OpenSearchRelNode at chain boundary but got [" + input.getClass().getSimpleName() + "]"
            );
        }
        RelTraitSet singletonTraits = input.getTraitSet().replace(distTraitDef.singleton());
        return new OpenSearchExchangeReducer(input.getCluster(), singletonTraits, input, openSearchInput.getViableBackends());
    }

    private static boolean isWindowedProject(RelNode node) {
        if (!(node instanceof OpenSearchProject project)) {
            return false;
        }
        WindowFunctionDetector detector = new WindowFunctionDetector();
        for (RexNode expr : project.getProjects()) {
            expr.accept(detector);
            if (detector.found) {
                return true;
            }
        }
        return false;
    }

    /**
     * Used by the upward walk: when a child below is windowed-affected, this operator must
     * also run on the coordinator (its input is now a gathered stream). Operators above the
     * windowed Project always need SINGLETON traits because they consume already-windowed
     * output that only exists post-gather.
     *
     * <p>The windowed Project itself returns false here because the {@link #walk} method has
     * a dedicated branch for it that flips the SINGLETON requirement on. A non-windowed
     * Project, Filter, or Sort sitting above the windowed Project does propagate.
     */
    private static boolean propagatesSingletonUpward(RelNode node) {
        if (node instanceof OpenSearchProject && isWindowedProject(node)) {
            return false;
        }
        return node instanceof OpenSearchSort || node instanceof OpenSearchProject || node instanceof OpenSearchFilter;
    }

    /**
     * Used by the downward walk: should the gather be inserted <em>below</em> this operator?
     * True only for operators whose semantics genuinely require a SINGLETON input — Sort
     * (global ordering / top-K) needs the entire stream on one node. Filter and column-
     * projection are row-local: running them per-shard before the gather is correct and
     * cheaper. Returning false here makes them boundaries, so the gather goes above them.
     */
    private static boolean requiresGatheredInput(RelNode node) {
        return node instanceof OpenSearchSort;
    }

    private static boolean inputsChanged(RelNode node, List<RelNode> rewrittenInputs) {
        List<RelNode> originalInputs = node.getInputs();
        for (int i = 0; i < originalInputs.size(); i++) {
            if (originalInputs.get(i) != rewrittenInputs.get(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the node already declares SINGLETON {@link OpenSearchDistribution}.
     * Used by {@link #lowerGatherInChain} to skip the gather insertion when the boundary is
     * already on a single node — single-shard scans are SINGLETON by construction.
     */
    private static boolean alreadySingleton(RelNode node) {
        for (int i = 0; i < node.getTraitSet().size(); i++) {
            if (node.getTraitSet().getTrait(i) instanceof OpenSearchDistribution distribution
                && distribution.getType() == RelDistribution.Type.SINGLETON) {
                return true;
            }
        }
        return false;
    }

    private static final class WindowFunctionDetector extends RexVisitorImpl<Void> {
        boolean found = false;

        WindowFunctionDetector() {
            super(true);
        }

        @Override
        public Void visitOver(RexOver over) {
            found = true;
            return null;
        }
    }

    /** Single-pass result: the (possibly-rebuilt) node and whether its subtree is windowed-affected. */
    private record Result(RelNode node, boolean windowedAffected) {}
}
