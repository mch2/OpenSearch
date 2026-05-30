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
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

/**
 * For an {@link OpenSearchSort} that orders or limits, requests SINGLETON input so the plan
 * becomes {@code Sort ← ER ← scan} — gather first, then sort/limit globally. Our ExchangeReducer
 * is a concat gather, not a merge exchange, so per-partition sort + concat is wrong, and so is a
 * per-partition {@code fetch} (it returns {@code fetch × partitions} rows). Pairs with the
 * SINGLETON-input cost gate in {@link OpenSearchSort#computeSelfCost} that makes the per-partition
 * alternative infinite-cost for any collated OR fetch/offset-bearing Sort.
 *
 * <p>A Sort that neither orders nor limits (no collation, no fetch, no offset) is left alone.
 *
 * @opensearch.internal
 */
public class OpenSearchSortSplitRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchSortSplitRule(PlannerContext context) {
        super(operand(OpenSearchSort.class, any()), "OpenSearchSortSplitRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        if (sort.getCollation().getFieldCollations().isEmpty() && sort.fetch == null && sort.offset == null) {
            return false; // neither orders nor limits — nothing to gather for
        }
        return !isSingleton(sort.getInput()) || !isSingleton(sort);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        RelTraitSet singletonTraits = sort.getTraitSet().replace(distTraitDef.coordSingleton());
        RelNode gatheredInput = convert(sort.getInput(), singletonTraits);
        call.transformTo(sort.copy(singletonTraits, gatheredInput, sort.getCollation(), sort.offset, sort.fetch));
    }

    private static boolean isSingleton(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) {
                return dist.getType() == RelDistribution.Type.SINGLETON;
            }
        }
        return false;
    }
}
