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
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.join.CoordinatorHashJoin;
import org.opensearch.analytics.planner.join.JoinStrategy;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * OpenSearch custom Join carrying viable backend list and a {@link JoinStrategy}.
 *
 * <p>The strategy decides how each side is distributed and where the join executes —
 * the rule attaches it; {@code DAGBuilder} doesn't need to know about join types.
 * Today the only strategy is {@link CoordinatorHashJoin}, which gathers both inputs
 * SINGLETON to the coordinator. Adding shuffle / broadcast strategies is purely
 * additive: implement {@code JoinStrategy}, select it in the rule.
 *
 * <p>Build-side contract: {@code right} input is always the build side. Calcite's
 * {@code LogicalJoin.right} maps to substrait {@code JoinRel.right}; users can
 * order inputs to control which side gets buffered. Stats-driven swap is a future
 * spec.
 *
 * @opensearch.internal
 */
public class OpenSearchJoin extends Join implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final JoinStrategy strategy;

    public OpenSearchJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<String> viableBackends,
        JoinStrategy strategy
    ) {
        super(cluster, traitSet, List.of(), left, right, condition, Set.of(), joinType);
        this.viableBackends = viableBackends;
        this.strategy = strategy;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Distribution strategy attached by the rule. Drives per-side ExchangeInfo and execution placement. */
    public JoinStrategy getStrategy() {
        return strategy;
    }

    /**
     * Output field storage is the concatenation of left and right input storage —
     * matches Calcite's join row type ordering (left fields first, then right).
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> result = new ArrayList<>();
        appendChildStorage(getLeft(), result);
        appendChildStorage(getRight(), result);
        return result;
    }

    private static void appendChildStorage(RelNode child, List<FieldStorageInfo> out) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(child);
        if (unwrapped instanceof OpenSearchRelNode os) {
            out.addAll(os.getOutputFieldStorage());
        }
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new OpenSearchJoin(getCluster(), traitSet, left, right, conditionExpr, joinType, viableBackends, strategy);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Constant non-zero cost — enough to keep Volcano from oscillating, not enough
        // to compete with future broadcast-join rules. Stats-driven costing is a future spec.
        return planner.getCostFactory().makeCost(100, 100, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends).item("strategy", strategy.getClass().getSimpleName());
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchJoin(
            getCluster(),
            getTraitSet(),
            children.get(0),
            children.get(1),
            getCondition(),
            getJoinType(),
            List.of(backend),
            strategy
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalJoin.create(
            strippedChildren.get(0),
            strippedChildren.get(1),
            List.of(),
            getCondition(),
            Set.<CorrelationId>of(),
            getJoinType()
        );
    }
}
