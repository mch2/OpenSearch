/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;

import java.util.List;

/**
 * Volcano CBO rule that wraps a windowed {@link OpenSearchProject} in a SINGLETON-conversion
 * request when its input is partitioned. A Project containing a {@link RexOver} computes a
 * window aggregate over a totally-ordered global stream — running sums, ranks, frame-bounded
 * aggregates, etc. Executing it over partitioned input is incorrect: each shard would produce
 * its own per-shard window state and the coordinator gather would concatenate mismatched
 * results.
 *
 * <p>Mirrors the {@link OpenSearchAggregateSplitRule} pattern: requests SINGLETON distribution
 * on the input via {@link RelOptRule#convert(RelNode, RelTraitSet)}, letting Volcano's trait
 * enforcement (via {@code ExpandConversionRule} + {@link
 * org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef}) automatically insert an
 * {@link org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer}. Pairs with the
 * complementary cost gate on {@link OpenSearchProject#computeSelfCost} which rejects the
 * non-SINGLETON-input alternative — together they force Volcano to pick the gathered-input
 * shape for any windowed Project.
 *
 * <p>TODO (FB-3 hash shuffle): once HASH_DISTRIBUTED exchanges are implemented, refine this to
 * allow HASH_DISTRIBUTED input when the {@link RexOver} carries a {@code PARTITION BY} clause
 * whose keys match the input's hash keys. SINGLETON remains required for unpartitioned RexOver.
 *
 * @opensearch.internal
 */
public class OpenSearchProjectGatherRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchProjectGatherRule(PlannerContext context) {
        super(operand(OpenSearchProject.class, operand(RelNode.class, any())), "OpenSearchProjectGatherRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        if (!containsWindowFunction(project.getProjects())) {
            return false;
        }
        return !inputIsSingletonOrAny(project.getInput());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        RelNode input = call.rel(1);

        // Construct the gather reducer explicitly rather than relying on
        // convert() + ExpandConversionRule. ExpandConversionRule does not reliably
        // materialize an intermediate SINGLETON converter for an input subset that is
        // already populated with a partitioned alternative — leaving the SINGLETON
        // subset empty and the conversion request unsatisfied. Mirror the explicit
        // shape that OpenSearchDistributionTraitDef.convert() would produce.
        RelTraitSet singletonTraits = input.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            registry,
            ((OpenSearchRelNode) input).getViableBackends()
        );
        OpenSearchExchangeReducer gathered = new OpenSearchExchangeReducer(input.getCluster(), singletonTraits, input, reduceViable);

        OpenSearchProject gatheredProject = (OpenSearchProject) project.copy(singletonTraits, gathered, project.getProjects(), project.getRowType());
        call.transformTo(gatheredProject);
    }

    private static boolean containsWindowFunction(java.util.List<RexNode> exprs) {
        WindowFunctionDetector detector = new WindowFunctionDetector();
        for (RexNode expr : exprs) {
            expr.accept(detector);
            if (detector.found) {
                return true;
            }
        }
        return false;
    }

    private static boolean inputIsSingletonOrAny(RelNode input) {
        for (int i = 0; i < input.getTraitSet().size(); i++) {
            RelTrait trait = input.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
                return distribution.getType() == RelDistribution.Type.SINGLETON || distribution.getType() == RelDistribution.Type.ANY;
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
}
