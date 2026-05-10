/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom Sort carrying viable backend list.
 *
 * @opensearch.internal
 */
public class OpenSearchSort extends Sort implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input, collation, offset, fetch);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Sort doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new OpenSearchSort(getCluster(), traitSet, input, collation, offset, fetch, viableBackends);
    }

    /**
     * Override Calcite's default — Calcite treats a "pure" Sort (has collation, no fetch/offset)
     * as a collation-trait enforcer, which causes Volcano to register it via
     * {@code getOrCreateSubset(traits, required=true)} and never call {@code setDelivered()}
     * on its RelSubset. The subset stays required-only, which breaks the
     * {@code addConverters} filter that looks for delivered subsets when converting an
     * inner Sort's RelSet to SINGLETON (gather rule path). We don't use Calcite's collation
     * trait infrastructure — our Sort is a concrete physical operator that should be marked
     * DELIVERED on registration like any other operator.
     */
    @Override
    public boolean isEnforcer() {
        return false;
    }

    /**
     * A Sort over non-SINGLETON input only sorts within each partition; the plain
     * {@link OpenSearchExchangeReducer} we use for gather concatenates partitions in
     * arbitrary order and is not a merge-sort exchange. So a non-SINGLETON Sort
     * doesn't produce a globally sorted result, which is the entire purpose of Sort.
     * Returning infinite cost on non-SINGLETON inputs forces Volcano to pick the
     * SINGLETON-input alternative produced by
     * {@link org.opensearch.analytics.planner.rules.OpenSearchSortDistributionDeriveRule}
     * (Sort over ER over RANDOM child) instead of the cheaper-looking but incorrect
     * "Sort over RANDOM + top-level ER" plan.
     *
     * <p>Once a SortedExchange / merge-sort gather exists, this can drop to a finite
     * additive cost so the planner can pick by I/O.
     */
    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(
        org.apache.calcite.plan.RelOptPlanner planner,
        org.apache.calcite.rel.metadata.RelMetadataQuery mq
    ) {
        for (org.apache.calcite.rel.RelNode input : getInputs()) {
            for (int i = 0; i < input.getTraitSet().size(); i++) {
                org.apache.calcite.plan.RelTrait trait = input.getTraitSet().getTrait(i);
                if (trait instanceof OpenSearchDistribution dist
                    && dist.getType() != org.apache.calcite.rel.RelDistribution.Type.SINGLETON
                    && dist.getType() != org.apache.calcite.rel.RelDistribution.Type.ANY) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchSort(getCluster(), getTraitSet(), children.getFirst(), getCollation(), offset, fetch, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalSort.create(strippedChildren.getFirst(), getCollation(), offset, fetch);
    }
}
