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
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.join.CoordinatorHashJoin;
import org.opensearch.analytics.planner.join.JoinStrategy;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.JoinCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP marker rewriting {@link LogicalJoin} → {@link OpenSearchJoin}. Picks a
 * {@link JoinStrategy} (today: {@link CoordinatorHashJoin}) and wraps each input in an
 * {@link OpenSearchExchangeReducer} so DAGBuilder cuts a per-side child stage;
 * redundant ERs over already-SINGLETON inputs dedupe via ConverterImpl.
 *
 * <p>Accepts INNER / LEFT / RIGHT / FULL equi-joins (FULL is needed by PPL
 * {@code appendcol}). Cross joins match via {@link JoinInfo#isEqui()}. Pure non-equi
 * predicates, SEMI, and ANTI are not yet enabled pending DataFusion validation.
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
        // Accept INNER / LEFT / RIGHT / FULL equi-joins. FULL is needed by PPL's
        // `appendcol` lowering, which pairs the outer pipeline with a subsearch via
        // ROW_NUMBER() OVER () and a full outer join on the row numbers. SEMI / ANTI
        // remain out until DataFusion execution is verified for them; the toJoinKind
        // mapping covers all four accepted kinds plus SEMI / ANTI for future use.
        if (joinType != JoinRelType.INNER
            && joinType != JoinRelType.LEFT
            && joinType != JoinRelType.RIGHT
            && joinType != JoinRelType.FULL) {
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

        // Viable backends = intersection of inputs' viable backends, narrowed to those whose
        // joinCapabilities declare the join's required JoinKind. Inputs are HepRelVertex-
        // wrapped marked nodes by the time this rule fires; bottom-up HEP traversal
        // guarantees they're already in OpenSearchConvention.
        List<String> viableBackends = computeViableBackends(join.getLeft(), join.getRight());
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
        // Wrap both inputs in an OpenSearchExchangeReducer at HEP time so DAGBuilder cuts
        // a separate child stage per side — same pattern as OpenSearchUnionRule. If the
        // input already delivers SINGLETON (e.g. a FINAL aggregate's output), the
        // ConverterImpl-based ER dedupes into the same RelSet subset.
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(context.getCapabilityRegistry(), viableBackends);
        RelNode leftUnwrapped = RelNodeUtils.unwrapHep(join.getLeft());
        RelNode rightUnwrapped = RelNodeUtils.unwrapHep(join.getRight());
        RelTraitSet leftSingletonTraits = leftUnwrapped.getTraitSet().replace(distTraitDef.singleton());
        RelTraitSet rightSingletonTraits = rightUnwrapped.getTraitSet().replace(distTraitDef.singleton());
        RelNode gatheredLeft = new OpenSearchExchangeReducer(join.getCluster(), leftSingletonTraits, leftUnwrapped, reduceViable);
        RelNode gatheredRight = new OpenSearchExchangeReducer(join.getCluster(), rightSingletonTraits, rightUnwrapped, reduceViable);

        RelTraitSet joinTraits = gatheredLeft.getTraitSet().replace(distTraitDef.singleton());
        OpenSearchJoin osJoin = new OpenSearchJoin(
            join.getCluster(),
            joinTraits,
            gatheredLeft,
            gatheredRight,
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
     *  {@link OpenSearchRelNode}. onMatch then narrows to backends whose
     *  {@link JoinCapability} declares the join's required kind. */
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
