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
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;

/**
 * Volcano CBO rule that produces a SINGLETON variant of an {@link OpenSearchFilter} so
 * SINGLETON requirements propagate through it. Mirrors
 * {@link OpenSearchSortDistributionDeriveRule} and
 * {@link OpenSearchProjectDistributionDeriveRule}.
 *
 * <p>Without this, a query like {@code ... | eventstats max(x) as mx | where mx > 5 |
 * head 1} can't construct a finite-cost SINGLETON path through the Filter sitting between
 * the windowed Project (gathered to SINGLETON) and the root.
 *
 * @opensearch.internal
 */
public class OpenSearchFilterDistributionDeriveRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchFilterDistributionDeriveRule(PlannerContext context) {
        super(operand(OpenSearchFilter.class, any()), "OpenSearchFilterDistributionDeriveRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchFilter filter = call.rel(0);
        return !traitsContainSingleton(filter.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchFilter filter = call.rel(0);
        RelTraitSet singletonTraits = filter.getTraitSet().replace(distTraitDef.singleton());
        RelNode singletonInput = convert(filter.getInput(), singletonTraits);
        RelNode newFilter = filter.copy(singletonTraits, singletonInput, filter.getCondition());
        call.transformTo(newFilter);
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
}
