/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
    /** Shard-local top-K — per-partition sort+limit; bypasses the gather-first gate in {@link #computeSelfCost}. */
    private final boolean localTopK;
    /** Per-collation-field RexNodes; lifted into a Project below the Sort by the convertor. {@code null} for plain field-index collation. */
    private final List<RexNode> sortExprs;

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends
    ) {
        this(cluster, traitSet, input, collation, offset, fetch, viableBackends, false, null);
    }

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends,
        boolean localTopK,
        List<RexNode> sortExprs
    ) {
        super(cluster, traitSet, input, collation, offset, fetch);
        this.viableBackends = viableBackends;
        this.localTopK = localTopK;
        if (sortExprs != null && sortExprs.size() != collation.getFieldCollations().size()) {
            throw new IllegalArgumentException(
                "sortExprs arity ["
                    + sortExprs.size()
                    + "] must match collation field count ["
                    + collation.getFieldCollations().size()
                    + "]"
            );
        }
        this.sortExprs = sortExprs == null ? null : List.copyOf(sortExprs);
    }

    /** True when this Sort is a shard-local top-K — see {@link #localTopK}. */
    public boolean isLocalTopK() {
        return localTopK;
    }

    /**
     * Returns the per-field expression-based sort keys, parallel to {@link #getCollation()}'s
     * {@code FieldCollation}s. {@code null} when collation is plain field-index collation.
     */
    public List<RexNode> getSortExprs() {
        return sortExprs;
    }

    /** True when this Sort carries expression-based sort keys (see {@link #sortExprs}). */
    public boolean hasExpressionCollation() {
        return sortExprs != null && !sortExprs.isEmpty();
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
        return new OpenSearchSort(getCluster(), traitSet, input, collation, offset, fetch, viableBackends, localTopK, sortExprs);
    }

    /** Concrete physical operator, not a collation enforcer — Volcano otherwise registers an undelivered required-subset that breaks the gather-rule path. */
    @Override
    public boolean isEnforcer() {
        return false;
    }

    /**
     * A collated Sort needs globally-ordered input — our {@link OpenSearchExchangeReducer} is a concat gather (not merge exchange),
     * so we require SINGLETON input. Pure-LIMIT (empty collation) and {@link #localTopK} Sorts skip the gate (partition-local fetch / top-K is correct).
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (getCollation().getFieldCollations().isEmpty()) {
            return planner.getCostFactory().makeTinyCost();
        }
        if (localTopK) {
            return planner.getCostFactory().makeTinyCost();
        }
        for (RelNode input : getInputs()) {
            for (int i = 0; i < input.getTraitSet().size(); i++) {
                RelTrait trait = input.getTraitSet().getTrait(i);
                if (trait instanceof OpenSearchDistribution distribution) {
                    boolean singletonOrAny = distribution.getType() == RelDistribution.Type.SINGLETON
                        || distribution.getType() == RelDistribution.Type.ANY;
                    if (!singletonOrAny) {
                        return planner.getCostFactory().makeInfiniteCost();
                    }
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter base = super.explainTerms(pw).item("viableBackends", viableBackends);
        if (sortExprs != null) {
            // In the digest so Volcano doesn't equivalence-class hinted vs un-hinted Sorts.
            base = base.item("sortExprs", sortExprs);
        }
        return base;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchSort(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            getCollation(),
            offset,
            fetch,
            List.of(backend),
            localTopK,
            sortExprs
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalSort.create(strippedChildren.getFirst(), getCollation(), offset, fetch);
    }
}
