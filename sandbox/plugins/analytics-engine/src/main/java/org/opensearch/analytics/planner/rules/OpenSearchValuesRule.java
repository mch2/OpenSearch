/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchValues;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link Values} → {@link OpenSearchValues}.
 *
 * <p>Marks empty-relation Values nodes (produced when Calcite's
 * {@code FilterReduceExpressionsRule} collapses a contradiction predicate like
 * {@code WHERE 1=2} into a zero-tuple Values) so parent rules see a marked
 * {@link org.opensearch.analytics.planner.rel.OpenSearchRelNode} child.
 *
 * <p>Values is format-agnostic; viable backends = all scan-capable backends from
 * the registry.
 *
 * @opensearch.internal
 */
public class OpenSearchValuesRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchValuesRule(PlannerContext context) {
        super(operand(Values.class, none()), "OpenSearchValuesRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Values values = call.rel(0);
        if (values instanceof OpenSearchValues) {
            return;
        }

        List<String> viableBackends = new ArrayList<>(context.getCapabilityRegistry().scanCapableBackends());
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No scan-capable backend registered — cannot mark Values");
        }

        @SuppressWarnings("unchecked")
        ImmutableList<ImmutableList<RexLiteral>> tuples = (ImmutableList<ImmutableList<RexLiteral>>) (ImmutableList<?>) values.getTuples();

        // Strip Calcite's default Values collation traits (Values registers a permutation
        // of every possible column ordering because any ordering is valid over zero rows).
        // Parent rules copy the child's traitSet; if a Sort inherits these permutations
        // its collation-vs-traitSet consistency check assertion-fails.
        RelTraitSet traits = values.getTraitSet().replace(RelCollations.EMPTY);

        call.transformTo(new OpenSearchValues(values.getCluster(), traits, values.getRowType(), tuples, viableBackends));
    }
}
