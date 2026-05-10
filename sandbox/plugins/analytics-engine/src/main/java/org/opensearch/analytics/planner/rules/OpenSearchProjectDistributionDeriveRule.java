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
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.List;

/**
 * Volcano CBO rule that produces a SINGLETON variant of a non-windowed
 * {@link OpenSearchProject} so SINGLETON requirements propagate through it. Mirrors
 * {@link OpenSearchSortDistributionDeriveRule}; skipped for windowed Projects (those are
 * gathered explicitly by {@link OpenSearchWindowedProjectGatherRule}).
 *
 * @opensearch.internal
 */
public class OpenSearchProjectDistributionDeriveRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchProjectDistributionDeriveRule(PlannerContext context) {
        super(operand(OpenSearchProject.class, any()), "OpenSearchProjectDistributionDeriveRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        if (containsRexOver(project.getProjects())) return false; // gathered by windowed rule
        return !traitsContainSingleton(project.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        RelTraitSet singletonTraits = project.getTraitSet().replace(distTraitDef.singleton());
        RelNode singletonInput = convert(project.getInput(), singletonTraits);
        RelNode newProject = project.copy(singletonTraits, List.of(singletonInput));
        call.transformTo(newProject);
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
