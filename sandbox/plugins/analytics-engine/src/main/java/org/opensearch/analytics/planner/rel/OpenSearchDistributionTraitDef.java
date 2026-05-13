/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;

import java.util.List;

/**
 * Trait definition for OpenSearch distribution.
 *
 * <p>Called by Volcano via ExpandConversionRule when a distribution trait mismatch
 * is detected. Produces an {@link OpenSearchExchangeReducer} for SINGLETON demands.
 * HASH/RANGE shuffle exchanges are not yet implemented.
 *
 * <p>One instance per query — created by {@link PlannerContext}.
 *
 * @opensearch.internal
 */
public class OpenSearchDistributionTraitDef extends RelTraitDef<OpenSearchDistribution> {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchDistributionTraitDef.class);

    private final PlannerContext plannerContext;

    public OpenSearchDistributionTraitDef(PlannerContext plannerContext) {
        this.plannerContext = plannerContext;
    }

    // ---- Factory methods ----

    /** SINGLETON(GATHERED) — result of a runtime exchange: ER output, FINAL aggregate output,
     *  Join/Union output. Also the default for generic SINGLETON demands (root, AggregateSplit). */
    public OpenSearchDistribution singleton() {
        return new OpenSearchDistribution(this, RelDistribution.Type.SINGLETON, List.of(), OpenSearchDistribution.Origin.GATHERED);
    }

    /** SINGLETON(SCAN) — single-shard scan: data already lives on one node by storage layout. */
    public OpenSearchDistribution scanSingleton() {
        return new OpenSearchDistribution(this, RelDistribution.Type.SINGLETON, List.of(), OpenSearchDistribution.Origin.SCAN);
    }

    /** SINGLETON with null origin — "get onto one node, don't care how". Used by root demand
     *  and DeriveRule so single-shard scans (SCAN) and gathered pipelines (GATHERED) both
     *  satisfy it without an extra ER. */
    public OpenSearchDistribution singletonAnyOrigin() {
        return new OpenSearchDistribution(this, RelDistribution.Type.SINGLETON, List.of(), null);
    }

    /** RANDOM — multi-shard scan output. */
    public OpenSearchDistribution random() {
        return new OpenSearchDistribution(this, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), null);
    }

    /** ANY — universal sink; any distribution satisfies it. Used as {@link #getDefault}. */
    public OpenSearchDistribution any() {
        return new OpenSearchDistribution(this, RelDistribution.Type.ANY, List.of(), null);
    }

    public OpenSearchDistribution hash(List<Integer> keys) {
        return new OpenSearchDistribution(this, RelDistribution.Type.HASH_DISTRIBUTED, keys, null);
    }

    /** Copies a distribution from another trait def — preserves type, keys, and origin. */
    public OpenSearchDistribution from(OpenSearchDistribution other) {
        return new OpenSearchDistribution(this, other.getType(), other.getKeys(), other.getOrigin());
    }

    public OpenSearchDistribution fromType(RelDistribution.Type type, List<Integer> keys) {
        OpenSearchDistribution.Origin origin = type == RelDistribution.Type.SINGLETON ? OpenSearchDistribution.Origin.GATHERED : null;
        return new OpenSearchDistribution(this, type, keys, origin);
    }

    // ---- RelTraitDef ----

    @Override
    public Class<OpenSearchDistribution> getTraitClass() {
        return OpenSearchDistribution.class;
    }

    @Override
    public String getSimpleName() {
        return "dist";
    }

    @Override
    public OpenSearchDistribution getDefault() {
        return any();
    }

    @Override
    public RelNode convert(RelOptPlanner planner, RelNode rel, OpenSearchDistribution toTrait, boolean allowInfiniteCostConverters) {
        OpenSearchDistribution fromTrait = rel.getTraitSet().getTrait(this);

        if (toTrait.getType() == RelDistribution.Type.ANY) {
            return rel;
        }

        if (fromTrait != null && fromTrait.satisfies(toTrait)) {
            return rel;
        }

        List<String> viableBackends = resolveViableBackendsFromRel(rel);

        LOGGER.info(
            "convert(): rel={}#{}, fromTrait={}, toTrait={}, backend={}",
            rel.getClass().getSimpleName(),
            rel.getId(),
            fromTrait,
            toTrait,
            viableBackends.getFirst()
        );

        CapabilityRegistry registry = plannerContext.getCapabilityRegistry();

        RelNode result;
        if (toTrait.getType() == RelDistribution.Type.SINGLETON) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, viableBackends);
            result = new OpenSearchExchangeReducer(rel.getCluster(), rel.getTraitSet().replace(toTrait), rel, reduceViable);
        } else {
            // TODO: implement HASH/RANGE shuffle exchange when joins and shuffle aggregates are added.
            throw new UnsupportedOperationException("HASH/RANGE exchange not yet implemented [toTrait=" + toTrait + "]");
        }

        return planner.register(result, rel);
    }

    @Override
    public boolean canConvert(RelOptPlanner planner, OpenSearchDistribution fromTrait, OpenSearchDistribution toTrait) {
        return true;
    }

    private static List<String> resolveViableBackendsFromRel(RelNode rel) {
        if (rel instanceof RelSubset subset) {
            rel = subset.getBestOrOriginal();
        }
        if (rel instanceof OpenSearchRelNode openSearchRel) {
            return openSearchRel.getViableBackends();
        }
        throw new IllegalStateException("Expected OpenSearchRelNode but got [" + rel.getClass().getSimpleName() + "#" + rel.getId() + "]");
    }
}
