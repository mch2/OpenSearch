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
import org.opensearch.analytics.planner.rel.OpenSearchSort;

/**
 * Volcano CBO rule that produces a SINGLETON variant of an {@link OpenSearchSort} so
 * the SINGLETON requirement can propagate through the Sort to its child.
 *
 * <p>When a windowed gather rule produces a SINGLETON variant in an inner subset (e.g.
 * eventstats's RelSet gets a SINGLETON subset), a Sort sitting above it stays in its
 * original RANDOM subset with input pointing at the input's RANDOM subset (infinite cost).
 * The root SINGLETON request then has no finite path through the Sort. This rule fires
 * once per RANDOM Sort and emits a SINGLETON-traited variant whose input is the
 * SINGLETON-converted input subset — Volcano's machinery (AbstractConverter +
 * ExpandConversionRule) builds an ER below if the input doesn't already have a SINGLETON
 * variant.
 *
 * <p>Mirrors Drill's per-Prule distribution-fan-out pattern in spirit, but specialized
 * for "SINGLETON only" since our trait def doesn't yet support HASH/RANGE shuffles.
 *
 * @opensearch.internal
 */
public class OpenSearchSortDistributionDeriveRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchSortDistributionDeriveRule(PlannerContext context) {
        super(operand(OpenSearchSort.class, any()), "OpenSearchSortDistributionDeriveRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        // Only fan out a SINGLETON variant when the sort isn't already SINGLETON.
        return !traitsContainSingleton(sort.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        RelTraitSet singletonTraits = sort.getTraitSet().replace(distTraitDef.singleton());
        RelNode singletonInput = convert(sort.getInput(), singletonTraits);
        RelNode newSort = sort.copy(
            singletonTraits,
            singletonInput,
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );
        call.transformTo(newSort);
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
