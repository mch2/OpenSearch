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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.StandardAggregateDecompositions;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.spi.AggregateDecomposition;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Volcano CBO rule that bridges a SINGLE-mode {@link OpenSearchAggregate} from RANDOM
 * input to SINGLETON output. Two paths depending on whether the calls are distributive:
 *
 * <ul>
 *   <li><b>Split path</b> (distributive calls): PARTIAL aggregate per shard, Exchange,
 *       FINAL aggregate at coordinator. Per-shard reduction shrinks the bytes shipped
 *       across the wire.</li>
 *   <li><b>Gather path</b> (non-distributive: distinct, UDAFs like TAKE, etc.): insert
 *       an Exchange under the original SINGLE aggregate so all rows arrive at the
 *       coordinator and the aggregate runs once. No partial state — correctness floor
 *       for cases where naïve identity-merge would be wrong (e.g. summing per-shard
 *       distinct counts double-counts values shared across shards).</li>
 * </ul>
 *
 * <p>Both paths request SINGLETON via {@code convert(...)}, letting Volcano's trait
 * enforcement insert an {@code OpenSearchExchangeReducer} automatically.
 *
 * <p>The PARTIAL and FINAL fragments emit <em>identical</em> aggregate calls — there is
 * no Java-side function decomposition (AVG → SUM/COUNT) and no COUNT → SUM0 trick. The
 * native backend's physical-plan walker rewrites every {@code AggregateExec.mode} based
 * on which side runs (Partial on data node, Final on coordinator). DataFusion's
 * {@code AggregateUDF::state_fields} machinery defines the wire schema between fragments
 * for every aggregate it knows how to split — including AVG (sum, count), STDDEV
 * (sum, sum_sq, count), HLL approx_distinct (sketch: Binary), TDigest percentile, and
 * custom UDAFs that implement state_fields.
 *
 * <p>What still falls to the gather path: distinct aggregates (`isDistinct()`) and any
 * aggregate whose state isn't naturally partition-distributive — see {@link #canSplit}.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        if (aggregate.getMode() != AggregateMode.SINGLE) {
            return false;
        }
        // Skip if input is already gathered — neither path is needed and the split path's
        // FINAL constructor re-validates agg call types against its (re-aggregated) input,
        // which fails for non-distributive aggregates whose partial state shape differs
        // from their result shape.
        RelNode input = call.rel(1);
        for (int i = 0; i < input.getTraitSet().size(); i++) {
            if (input.getTraitSet().getTrait(i) instanceof OpenSearchDistribution dist) {
                if (dist.getType() == RelDistribution.Type.SINGLETON || dist.getType() == RelDistribution.Type.ANY) {
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    /** True iff every aggCall can be split into Partial + Final stages. The native
     *  side strips DataFusion's auto-generated `Partial` on coord (so `Final` reads
     *  state directly) and strips the auto-generated `Final` on data node (so the
     *  `Partial` emits state). Each function's partial-state shape is declared via
     *  {@link org.opensearch.analytics.spi.AggregateDecomposition} on the backend's
     *  {@link org.opensearch.analytics.spi.AggregateCapability}; the default
     *  (state == result) covers SUM/MIN/MAX/COUNT, and backends override for AVG,
     *  STDDEV, HLL, etc.
     *
     *  <p>Only distinct aggregates fall to the gather path — their state is
     *  unbounded (a hash set / list of all distinct values) and cheaper to dedup
     *  centrally than to ship from every shard. (HLL-based approximate distinct
     *  count is splittable; it's a separate function name with bounded sketch
     *  state.) */
    private static boolean canSplit(OpenSearchAggregate aggregate) {
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (aggCall.isDistinct()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        if (canSplit(aggregate)) {
            applySplit(call, aggregate, child);
        } else {
            applyGather(call, aggregate, child);
        }
    }

    /** Split path: PARTIAL → Exchange → FINAL. Both fragments carry identical aggregate
     *  calls — the native physical-plan walker decides Partial vs Final at execution time
     *  based on which side runs.
     *
     *  <p>The PARTIAL aggregate's row type is overridden using the chosen backend's
     *  {@link AggregateDecomposition#partialStateSchema} for each measure. That schema
     *  flows through the Exchange into the FINAL fragment's {@code StageInputScan} row
     *  type, into the substrait {@code NamedScan.base_schema}, and onto the streaming
     *  table the native coord registers — keeping every layer consistent on the
     *  state-shape DataFusion's {@code AggregateExec(Final)} actually produces. */
    private void applySplit(RelOptRuleCall call, OpenSearchAggregate aggregate, RelNode child) {
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        RelDataType partialRowType = computePartialRowType(aggregate, child);

        // Partial aggregate: runs on each partition, keeps input's traits
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggCalls,
            AggregateMode.PARTIAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations(),
            partialRowType
        );

        // Request SINGLETON distribution — Volcano inserts Exchange automatically
        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        // Final aggregate: same calls as partial. Native side runs this with mode=Final;
        // the input from the exchange carries the partial state schema and the Final-mode
        // AggregateExec consumes them as state, not raw values.
        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );

        call.transformTo(finalAggregate);
    }

    /** Computes the PARTIAL fragment's output row type from the chosen backend's
     *  {@link AggregateDecomposition} declarations. Resulting row type is
     *  {@code groupBy fields ++ flatten(partialStateSchema per measure)}. */
    private RelDataType computePartialRowType(OpenSearchAggregate aggregate, RelNode child) {
        RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
        CapabilityRegistry registry = context.getCapabilityRegistry();
        // Pick the first viable backend. For multi-backend cases this resolves at plan
        // forking time; here we just need a single backend's decomposition declarations.
        // Different backends declaring different state shapes for the same function would
        // require plan forking to keep them separate.
        String backend = aggregate.getViableBackends().getFirst();

        List<RelDataTypeField> partialFields = new ArrayList<>();
        // Group-by fields preserve their input types & names.
        RelDataType inputType = child.getRowType();
        for (int groupIdx : aggregate.getGroupSet()) {
            partialFields.add(inputType.getFieldList().get(groupIdx));
        }
        // Per-measure state fields, flattened.
        int idx = partialFields.size();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            AggregateFunction fn = AggregateFunction.resolve(aggCall);
            AggregateDecomposition decomposition = StandardAggregateDecompositions.orDefault(registry.aggregateDecomposition(backend, fn));
            RelDataType stateRowType = decomposition.partialStateSchema(aggCall, typeFactory);
            for (RelDataTypeField stateField : stateRowType.getFieldList()) {
                partialFields.add(new org.apache.calcite.rel.type.RelDataTypeFieldImpl(stateField.getName(), idx++, stateField.getType()));
            }
        }
        return typeFactory.createStructType(
            partialFields.stream().map(RelDataTypeField::getType).toList(),
            partialFields.stream().map(RelDataTypeField::getName).toList()
        );
    }

    /** Gather path: Exchange → SINGLE-mode aggregate. No decomposition; runs as-is at
     *  the coordinator over all gathered rows. Cost is shipping every row to one node,
     *  but it's the only correctness-safe option for distinct/UDAFs without a backend
     *  AggregateDecomposition registered. */
    private void applyGather(RelOptRuleCall call, OpenSearchAggregate aggregate, RelNode child) {
        RelTraitSet singletonTraits = child.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(child, singletonTraits);
        OpenSearchAggregate gatheredSingle = new OpenSearchAggregate(
            aggregate.getCluster(),
            aggregate.getTraitSet(),
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.SINGLE,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );
        call.transformTo(gatheredSingle);
    }
}
