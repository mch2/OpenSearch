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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.join.CoordinatorHashJoin;
import org.opensearch.analytics.planner.join.JoinStrategy;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.JoinCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP-marker rule that converts a Calcite {@link LogicalJoin} into an
 * {@link OpenSearchJoin} in {@link OpenSearchConvention}.
 *
 * <p>Strategy selection: the rule picks a {@link JoinStrategy} for the join (today
 * always {@link CoordinatorHashJoin} — stats / hint-driven selection is a future
 * spec) and attaches it to the {@link OpenSearchJoin}. Adding shuffle / broadcast
 * strategies later requires implementing {@link JoinStrategy} and adjusting selection
 * here — no DAGBuilder changes needed.
 *
 * <p>Gather insertion: this HEP rule emits the {@code OpenSearchJoin} with raw inputs.
 * The Volcano-CBO {@link OpenSearchJoinGatherRule} requests SINGLETON on each input via
 * {@code convert(input, SINGLETON)}; {@code AbstractConverter.ExpandConversionRule} +
 * {@code OpenSearchDistributionTraitDef.convert} insert
 * {@code OpenSearchExchangeReducer}s only when the input distribution doesn't already
 * satisfy SINGLETON.
 *
 * <p><b>Match criteria</b> (Requirement 1):
 * <ul>
 *   <li>{@link JoinRelType#INNER}, {@link JoinRelType#LEFT}, or {@link JoinRelType#RIGHT}.
 *       FULL OUTER / SEMI / ANTI remain out of scope until their DataFusion substrait
 *       execution paths are validated end-to-end.</li>
 *   <li>Equi-condition only ({@link JoinInfo#isEqui()} true). This covers both the
 *       normal case (at least one equi-clause) and the degenerate cross join shape
 *       (empty leftKeys AND empty nonEquiConditions — e.g. {@code ON 1=1}). Isthmus
 *       emits the latter as a substrait {@code Cross} rel, which DataFusion executes
 *       as a NestedLoopJoin. Pure non-equi joins (e.g. {@code t1.a &lt; t2.b}, where
 *       nonEquiConditions is non-empty) are still rejected — they'd need a separate
 *       non-equi codepath we don't enable yet.</li>
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
        // Accept equi-joins and cross joins (both satisfy JoinInfo.isEqui() — empty
        // nonEquiConditions). A pure non-equi predicate (e.g. t1.a < t2.b) yields
        // isEqui()=false and stays rejected — DataFusion would need a non-equi
        // NestedLoopJoin path we don't enable yet.
        JoinInfo info = join.analyzeCondition();
        return info.isEqui();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        // Compute viable backends as the intersection of the inputs' viable backends,
        // then retain only backends that declare JOIN capability. Inputs are
        // HepRelVertex-wrapped marked nodes (OpenSearchTableScan etc.) by the time this
        // rule fires; the bottom-up HEP traversal guarantees they're already in
        // OpenSearchConvention.
        List<String> viableBackends = computeViableBackends(join.getLeft(), join.getRight());
        List<String> joinCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.JOIN);
        viableBackends.retainAll(joinCapable);
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports JOIN among viable backends after intersecting inputs");
        }

        // Pick a strategy. Today only CoordinatorHashJoin exists; future work picks based on
        // stats/hints. Narrow viable backends to those whose joinCapabilities declare the
        // join's required JoinKind — a backend with EngineCapability.JOIN but no support
        // for (say) LEFT outer is dropped.
        JoinStrategy strategy = new CoordinatorHashJoin();
        JoinCapability.JoinKind requiredKind = toJoinKind(join.getJoinType());
        viableBackends.removeIf(backend -> {
            var caps = context.getCapabilityRegistry().getBackend(backend).getCapabilityProvider();
            for (JoinCapability cap : caps.joinCapabilities()) {
                if (cap.kinds().contains(requiredKind)) return false;
            }
            return true;
        });
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports join kind [" + requiredKind + "] among viable backends");
        }
        // Trait set for the marked join. CoordinatorHashJoin gathers SINGLETON; this is
        // currently the only strategy and the trait set reflects that. When non-singleton
        // strategies land, the join's own distribution trait will need to come from the
        // strategy too (it's currently implicit in the call to distributionTraitDef.singleton()).
        RelTraitSet joinTraits = join.getTraitSet()
            .replace(OpenSearchConvention.INSTANCE)
            .replace(context.getDistributionTraitDef().singleton());

        // Inputs are passed through unwrapped — gather insertion is handled by
        // OpenSearchJoinGatherRule during Volcano CBO via convert(input, SINGLETON).
        OpenSearchJoin osJoin = new OpenSearchJoin(
            join.getCluster(),
            joinTraits,
            RelNodeUtils.unwrapHep(join.getLeft()),
            RelNodeUtils.unwrapHep(join.getRight()),
            join.getCondition(),
            join.getJoinType(),
            viableBackends,
            strategy
        );
        call.transformTo(osJoin);
    }

    private static JoinCapability.JoinKind toJoinKind(JoinRelType joinType) {
        return switch (joinType) {
            case INNER -> JoinCapability.JoinKind.INNER;
            case LEFT -> JoinCapability.JoinKind.LEFT;
            case RIGHT -> JoinCapability.JoinKind.RIGHT;
            case FULL -> JoinCapability.JoinKind.FULL;
            case SEMI -> JoinCapability.JoinKind.SEMI;
            case ANTI -> JoinCapability.JoinKind.ANTI;
            // Calcite's JoinRelType has variants beyond standard SQL. Routes here aren't
            // enabled — matches() only accepts INNER/LEFT/RIGHT today.
            default -> throw new IllegalStateException("Unhandled JoinRelType: " + joinType);
        };
    }

    /** Intersection of viable backends from left and right children. Children may be
     *  {@link HepRelVertex}-wrapped — unwrap to read viableBackends if it's an
     *  {@link OpenSearchRelNode}. onMatch then intersects with backends that declare
     *  {@link EngineCapability#JOIN} so a backend that happens to be viable on both
     *  inputs without supporting coord-side join is filtered out. */
    private static List<String> computeViableBackends(RelNode left, RelNode right) {
        List<String> leftBackends = viableBackendsOf(left);
        List<String> rightBackends = viableBackendsOf(right);

        Set<String> intersection = new LinkedHashSet<>(leftBackends);
        intersection.retainAll(rightBackends);
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
