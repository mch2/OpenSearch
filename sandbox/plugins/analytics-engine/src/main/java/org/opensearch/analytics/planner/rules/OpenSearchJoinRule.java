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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP-marker rule that converts a Calcite {@link LogicalJoin} into an
 * {@link OpenSearchJoin} in {@link OpenSearchConvention}.
 *
 * <p>The marked join wraps each input in an {@link OpenSearchExchangeReducer}
 * carrying SINGLETON distribution. The DAG builder cuts at every reducer, producing
 * one child stage per join input (a 3-stage DAG: 2 children + 1 coord parent).
 *
 * <p><b>Match criteria</b> (Requirement 1):
 * <ul>
 *   <li>{@link JoinRelType#INNER}, {@link JoinRelType#LEFT}, or {@link JoinRelType#RIGHT}.
 *       FULL OUTER / SEMI / ANTI remain out of scope until their DataFusion substrait
 *       execution paths are validated end-to-end.</li>
 *   <li>At least one equi-condition (analyzeCondition().leftKeys non-empty). Pure
 *       non-equi joins fall to a future spec.</li>
 * </ul>
 *
 * <p><b>Build-side contract</b> (Requirement 8): {@link LogicalJoin#getRight()} becomes
 * the {@code right} input of the substrait {@code JoinRel}, which is DataFusion's
 * default build side. Users can hint by ordering inputs as
 * {@code SELECT ... FROM <probe> JOIN <build> ON ...}. The Calcite rewrite does NOT
 * swap left/right based on size estimates — CBO-driven swap is a future spec.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchJoinRule(PlannerContext context) {
        super(operand(LogicalJoin.class, any()), "OpenSearchJoinRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        // Accept INNER / LEFT / RIGHT equi-joins. PPL's `lookup` command and other
        // outer-join-producing front ends rely on LEFT; widening here lets the
        // marking pass descend through the join for downstream rules. FULL / SEMI /
        // ANTI remain out until DataFusion execution is verified for them.
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT && joinType != JoinRelType.RIGHT) {
            return false;
        }
        // Require at least one equi-condition. Pure non-equi joins (e.g. t1.a < t2.b)
        // are out of scope for this spec — DataFusion would fall back to nested-loop
        // join, a separate codepath we don't enable yet.
        JoinInfo info = join.analyzeCondition();
        return !info.leftKeys.isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        // Compute viable backends as the intersection of the inputs' viable backends.
        // Inputs are HepRelVertex-wrapped marked nodes (OpenSearchTableScan etc.) by the
        // time this rule fires; the bottom-up HEP traversal guarantees they're already
        // in OpenSearchConvention.
        List<String> viableBackends = computeViableBackends(join.getLeft(), join.getRight());
        RelTraitSet singletonTraits = join.getTraitSet().replace(OpenSearchConvention.INSTANCE)
            .replace(context.getDistributionTraitDef().singleton());

        // Wrap each input in an OpenSearchExchangeReducer to gather it to the coord.
        // The DAG builder cuts at every reducer, producing one child stage per join input.
        // The join itself is SINGLETON since both gathered inputs are SINGLETON — declaring
        // this on the join's trait set lets any operator above it (e.g. Project) inherit
        // SINGLETON without Volcano inserting a redundant top-level reducer.
        RelNode left = wrapInExchange(join.getLeft(), singletonTraits, viableBackends);
        RelNode right = wrapInExchange(join.getRight(), singletonTraits, viableBackends);

        OpenSearchJoin osJoin = new OpenSearchJoin(
            join.getCluster(),
            singletonTraits,
            left,
            right,
            join.getCondition(),
            join.getJoinType(),
            viableBackends
        );
        call.transformTo(osJoin);
    }

    private static RelNode wrapInExchange(RelNode input, RelTraitSet singletonTraits, List<String> viableBackends) {
        return new OpenSearchExchangeReducer(input.getCluster(), singletonTraits, input, viableBackends);
    }

    /** Intersection of viable backends from left and right children. Children may be
     *  {@link HepRelVertex}-wrapped — unwrap to read viableBackends if it's an
     *  {@link OpenSearchRelNode}. */
    private static List<String> computeViableBackends(RelNode left, RelNode right) {
        List<String> leftBackends = viableBackendsOf(left);
        List<String> rightBackends = viableBackendsOf(right);

        Set<String> intersection = new LinkedHashSet<>(leftBackends);
        intersection.retainAll(rightBackends);
        if (intersection.isEmpty()) {
            // Fall back to the single backend that actually supports coord-side joins
            // today. If both inputs really have disjoint viable backends, that's a
            // capability-resolution concern for a future pass; for the IT target
            // (datafusion + datafusion) the intersection always has datafusion.
            return List.of("datafusion");
        }
        return new ArrayList<>(intersection);
    }

    private static List<String> viableBackendsOf(RelNode rel) {
        RelNode unwrapped = rel;
        if (unwrapped instanceof HepRelVertex vertex) {
            unwrapped = vertex.getCurrentRel();
        }
        if (unwrapped instanceof OpenSearchRelNode osNode) {
            return osNode.getViableBackends();
        }
        // Not yet marked — empty list forces the fallback path above.
        return List.of();
    }
}
