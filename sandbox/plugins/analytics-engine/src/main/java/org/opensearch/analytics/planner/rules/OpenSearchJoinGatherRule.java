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
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Volcano CBO rule that requests SINGLETON distribution on each {@link OpenSearchJoin}
 * input. The actual ER insertion is performed by
 * {@code AbstractConverter.ExpandConversionRule} +
 * {@link OpenSearchDistributionTraitDef#convert} — this rule just declares the requirement.
 *
 * <p>Idempotency: {@link #matches} returns false once both inputs already deliver
 * SINGLETON. Calling {@code convert(input, SINGLETON)} on a SINGLETON-delivered subset
 * is a no-op, but matching the rule on an already-gathered join would still re-fire
 * the call infrastructure; the trait check skips that work cleanly.
 *
 * <p>This rule presupposes that {@link org.opensearch.analytics.planner.rel.OpenSearchTableScan}
 * declares RANDOM distribution regardless of shard count. If a leaf rel honestly delivered
 * SINGLETON (e.g. a true coordinator-side rel), the rule would correctly skip it.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinGatherRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchJoinGatherRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchJoinGatherRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        return !isSingleton(join.getInput(0)) || !isSingleton(join.getInput(1));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        RelTraitSet singletonTraits = join.getTraitSet().replace(distTraitDef.singleton());
        RelNode newLeft = convert(join.getInput(0), singletonTraits);
        RelNode newRight = convert(join.getInput(1), singletonTraits);
        call.transformTo(
            join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(), join.isSemiJoinDone())
        );
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
