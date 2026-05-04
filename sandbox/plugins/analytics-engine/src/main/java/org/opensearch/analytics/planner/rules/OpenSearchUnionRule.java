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
import org.apache.calcite.rel.logical.LogicalUnion;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP-marker rule that converts a Calcite {@link LogicalUnion} into an
 * {@link OpenSearchUnion} in {@link OpenSearchConvention}.
 *
 * <p>Each input is wrapped in an {@link OpenSearchExchangeReducer} carrying SINGLETON
 * distribution, mirroring {@link OpenSearchJoinRule}. The DAG builder cuts at every
 * reducer, producing one child stage per union input (an (N+1)-stage DAG: N children
 * + one coord parent).
 *
 * <p><b>Match criteria</b>: every {@link LogicalUnion} matches. Both UNION ALL
 * (default for PPL command pipelines) and UNION DISTINCT are accepted — substrait
 * {@code SetRel} carries the distinctness flag natively, and DataFusion's substrait
 * consumer handles both forms.
 *
 * @opensearch.internal
 */
public class OpenSearchUnionRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchUnionRule(PlannerContext context) {
        super(operand(LogicalUnion.class, any()), "OpenSearchUnionRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);

        // Intersection of viable backends across every input. Inputs are already
        // marked by the bottom-up HEP traversal.
        List<String> viableBackends = computeViableBackends(union.getInputs());
        RelTraitSet singletonTraits = union.getTraitSet()
            .replace(OpenSearchConvention.INSTANCE)
            .replace(context.getDistributionTraitDef().singleton());

        // Wrap each input in an OpenSearchExchangeReducer — the DAG builder will cut
        // at every reducer, producing one child stage per input. The union itself is
        // SINGLETON because every gathered input is SINGLETON; declaring this on the
        // trait set lets operators above the union inherit SINGLETON without Volcano
        // inserting a redundant top-level reducer.
        List<RelNode> wrappedInputs = new ArrayList<>(union.getInputs().size());
        for (RelNode input : union.getInputs()) {
            wrappedInputs.add(new OpenSearchExchangeReducer(input.getCluster(), singletonTraits, input, viableBackends));
        }

        OpenSearchUnion osUnion = new OpenSearchUnion(
            union.getCluster(),
            singletonTraits,
            wrappedInputs,
            union.all,
            viableBackends
        );
        call.transformTo(osUnion);
    }

    /** Intersection of viable backends across all inputs. Children may be
     *  {@link HepRelVertex}-wrapped — unwrap to read viableBackends if it's an
     *  {@link OpenSearchRelNode}. Falls back to the single backend that supports
     *  coord-side set operations today ({@code datafusion}) when the intersection
     *  is empty, matching {@link OpenSearchJoinRule}'s fallback. */
    private static List<String> computeViableBackends(List<RelNode> inputs) {
        Set<String> intersection = null;
        for (RelNode input : inputs) {
            List<String> inputBackends = viableBackendsOf(input);
            if (intersection == null) {
                intersection = new LinkedHashSet<>(inputBackends);
            } else {
                intersection.retainAll(inputBackends);
            }
        }
        if (intersection == null || intersection.isEmpty()) {
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
        return List.of();
    }
}
