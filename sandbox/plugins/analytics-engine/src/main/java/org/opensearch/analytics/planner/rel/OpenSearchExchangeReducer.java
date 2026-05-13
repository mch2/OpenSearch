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
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Coordinator-side reducer for exchanges. Receives streaming Arrow batches from
 * upstream stages via Analytics Core transport. The backend decides internally
 * how to reduce (in-memory table, streaming sink, etc.).
 *
 * <p>Carries an {@link ExchangeInfo} describing the distribution it represents.
 * Defaults to {@link ExchangeInfo#singleton()}; callers (e.g. {@code OpenSearchJoinRule}
 * via a {@code JoinStrategy}) may construct it with HASH or RANDOM ExchangeInfo
 * for shuffle / broadcast variants. {@code DAGBuilder} reads the ExchangeInfo
 * directly off the reducer when cutting child stages — it never queries upstream
 * operators for distribution intent.
 *
 * <p>Only SINGLETON exchanges are wired end-to-end today. HASH/RANGE shuffle
 * execution is still TODO — see {@link OpenSearchDistributionTraitDef}.
 *
 * @opensearch.internal
 */
public class OpenSearchExchangeReducer extends ConverterImpl implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final ExchangeInfo exchangeInfo;

    /** Convenience constructor — defaults to {@link ExchangeInfo#singleton()}. */
    public OpenSearchExchangeReducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<String> viableBackends) {
        this(cluster, traitSet, input, viableBackends, ExchangeInfo.singleton());
    }

    public OpenSearchExchangeReducer(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> viableBackends,
        ExchangeInfo exchangeInfo
    ) {
        // ConverterImpl makes this a Calcite-recognized trait converter. When HEP wraps an ER
        // around a SINGLE aggregate and Volcano then splits the aggregate into FINAL (which
        // also delivers SINGLETON(GATHERED)), the ER and FINAL land in the same RelSet subset
        // and Volcano picks FINAL as the cheaper alternative — no redundant ER-over-FINAL.
        // Origin.SCAN keeps ERs over single-shard scans in a distinct subset so they survive
        // as stage-cut boundaries (e.g. per-arm Union / Join inputs).
        super(cluster, null, traitSet, input);
        this.viableBackends = viableBackends;
        this.exchangeInfo = exchangeInfo;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Distribution this reducer represents — read by DAGBuilder when cutting child stages. */
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchangeReducer(getCluster(), traitSet, sole(inputs), viableBackends, exchangeInfo);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends).item("exchange", exchangeInfo);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), children.getFirst(), List.of(backend), exchangeInfo);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // ExchangeReducer is an infrastructure node — strip children but keep the node itself.
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), strippedChildren.getFirst(), viableBackends, exchangeInfo);
    }
}
