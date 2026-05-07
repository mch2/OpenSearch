/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

/**
 * Calcite convention for all OpenSearch Analytics operators.
 * Operators using this convention participate in Volcano CBO
 * for distribution trait propagation and exchange insertion.
 *
 * @opensearch.internal
 */
public enum OpenSearchConvention implements Convention {
    INSTANCE;

    @Override
    public Class<?> getInterface() {
        return OpenSearchRelNode.class;
    }

    @Override
    public String getName() {
        return "OPENSEARCH";
    }

    @Override
    public RelTraitDef<Convention> getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return this == trait;
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
    }

    /**
     * Top-down Volcano calls {@code enforce} when an operator's required output traits cannot
     * be satisfied by any {@link org.apache.calcite.rel.PhysicalNode#passThroughTraits}-style
     * propagation through the existing tree — the framework needs to physically materialize
     * an enforcer (an exchange / converter) that converts whatever the child produces into
     * what the parent demands.
     *
     * <p>For our distribution traits, the only supported enforcer is the SINGLETON gather:
     * wrap the input in {@link OpenSearchExchangeReducer}, which is exactly what
     * {@link OpenSearchDistributionTraitDef#convert} does for bottom-up exchange insertion.
     * Returning {@code null} for unsupported conversions (HASH/RANGE) lets Volcano skip the
     * path; once shuffle exchanges land, this branches on the requested distribution type.
     *
     * <p>If the input already satisfies the required distribution we return it unchanged so
     * Volcano doesn't insert a redundant enforcer (matches the early-out
     * {@code OpenSearchDistributionTraitDef.convert} performs via {@code fromTrait.satisfies}).
     */
    @Override
    public RelNode enforce(RelNode input, RelTraitSet required) {
        OpenSearchDistribution requiredDist = traitOfType(required);
        if (requiredDist == null || requiredDist.getType() != RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistribution inputDist = traitOfType(input.getTraitSet());
        if (inputDist != null && inputDist.satisfies(requiredDist)) {
            return input;
        }
        if (!(input instanceof OpenSearchRelNode openSearchInput)) {
            return null;
        }
        return new OpenSearchExchangeReducer(input.getCluster(), required, input, openSearchInput.getViableBackends());
    }

    private static OpenSearchDistribution traitOfType(RelTraitSet traits) {
        for (int i = 0; i < traits.size(); i++) {
            RelTrait trait = traits.getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
                return distribution;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getName();
    }
}
