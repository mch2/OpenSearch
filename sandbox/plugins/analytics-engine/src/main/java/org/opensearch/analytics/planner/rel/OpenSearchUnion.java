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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom Union carrying viable backend list.
 *
 * <p>Like {@link OpenSearchJoin}, this spec only emits a coord-side set-op: every input
 * requests SINGLETON distribution so Volcano inserts an {@link OpenSearchExchangeReducer}
 * above each. The DAG builder cuts each reducer into its own child stage; the coordinator
 * stage's substrait fragment becomes a {@code SetRel(UNION_ALL)} (or {@code UNION_DISTINCT})
 * with one {@code NamedScan} per input (inputs are indexed {@code "input-0"},
 * {@code "input-1"}, ...).
 *
 * <p>Unions are produced by several PPL commands through {@code relBuilder.union(all)}:
 * {@code addcoltotals}, {@code addtotals}, {@code append}, {@code appendpipe},
 * {@code multisearch}. None of these emit custom RelNodes — they all lower to standard
 * Calcite {@link LogicalUnion}.
 *
 * @opensearch.internal
 */
public class OpenSearchUnion extends Union implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, List<String> viableBackends) {
        super(cluster, traitSet, inputs, all);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output field storage is taken from the first input's storage. Calcite's Union
     * requires every input to have a row-type-compatible structure, so per-column
     * storage metadata is identical across inputs (at least at the field-type /
     * format level).
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode first = RelNodeUtils.unwrapHep(getInputs().get(0));
        if (first instanceof OpenSearchRelNode os) {
            return os.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new OpenSearchUnion(getCluster(), traitSet, inputs, all, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Constant non-zero cost — same shape as OpenSearchJoin. Stats-driven costing is a
        // future spec.
        return planner.getCostFactory().makeCost(100, 100, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchUnion(getCluster(), getTraitSet(), children, all, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalUnion.create(strippedChildren, all);
    }
}
