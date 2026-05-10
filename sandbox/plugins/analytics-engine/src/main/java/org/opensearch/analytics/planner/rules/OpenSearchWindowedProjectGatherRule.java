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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.List;

/**
 * Volcano CBO rule that wraps a windowed {@link OpenSearchProject}'s input with an
 * {@link OpenSearchExchangeReducer} (SINGLETON gather).
 *
 * <p>A {@code Project} containing a {@link RexOver} computes a window aggregate over a
 * totally-ordered global stream — running sums, ranks, frame-bounded aggregates, etc.
 * Executing it over partitioned input is incorrect: each shard would produce its own
 * per-shard window state and the coordinator gather would concatenate mismatched results.
 *
 * <p>This rule directly constructs the {@code OpenSearchExchangeReducer} below the
 * windowed Project (mirrors Drill's {@code WindowPrule} ORDER-BY case which builds
 * {@code SingleMergeExchangePrel} explicitly). Going through {@code convert(child,
 * SINGLETON)} and relying on {@code AbstractConverter.ExpandConversionRule} works for
 * single windowed Projects but stops triggering reliably when windowed Projects stack
 * (the inner SINGLETON subset is created but Volcano never materialises a satisfying
 * alternative inside it, leaving an infinite-cost cascade).
 *
 * <p>Idempotency: when the child trait set already contains SINGLETON (FINAL Aggregate,
 * another windowed Project that already gathered, Join with SINGLETON inputs, single-shard
 * scan), no exchange is inserted — the rule rewrites the project in place with SINGLETON
 * traits.
 *
 * @opensearch.internal
 */
public class OpenSearchWindowedProjectGatherRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchWindowedProjectGatherRule(PlannerContext context) {
        super(operand(OpenSearchProject.class, operand(RelNode.class, any())), "OpenSearchWindowedProjectGatherRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        if (!containsRexOver(project.getProjects())) return false;
        RelNode child = call.rel(1);
        return !traitsContainSingleton(child.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        RelNode child = call.rel(1);

        // convert() ensures the SINGLETON subset of child's RelSet exists.
        RelTraitSet erTraits = child.getTraitSet().replace(distTraitDef.singleton());
        RelNode gathered = convert(child, erTraits);

        RelTraitSet projectTraits = project.getTraitSet().replace(distTraitDef.singleton());
        call.transformTo(project.copy(projectTraits, List.of(gathered)));
    }

    private static boolean containsRexOver(List<RexNode> exprs) {
        OverDetector detector = new OverDetector();
        for (RexNode expr : exprs) {
            expr.accept(detector);
            if (detector.found) return true;
        }
        return false;
    }

    private static boolean traitsContainSingleton(RelTraitSet traits) {
        for (int i = 0; i < traits.size(); i++) {
            if (traits.getTrait(i) instanceof OpenSearchDistribution dist
                && dist.getType() == RelDistribution.Type.SINGLETON) {
                return true;
            }
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
