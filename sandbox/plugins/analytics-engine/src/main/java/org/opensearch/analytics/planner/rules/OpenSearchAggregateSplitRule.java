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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Volcano CBO rule that splits an {@link OpenSearchAggregate} into
 * PARTIAL + FINAL when the input is partitioned.
 *
 * <p>Requests SINGLETON distribution on the partial output, letting Volcano's
 * trait enforcement (via {@code ExpandConversionRule} + {@code OpenSearchDistributionTraitDef})
 * automatically insert an {@code OpenSearchExchangeReducer}.
 *
 * <p>TODO (plan forking): aggregate decomposition is intentionally deferred to plan forking
 * resolution, after a single backend has been chosen per alternative. Decomposition is
 * backend-specific — different backends may emit different partial state schemas for the
 * same function (e.g. standard SUM+COUNT for AVG vs a backend's native running state).
 * Applying decomposition here would force a single schema before backends are resolved,
 * which breaks the multi-alternative model.
 *
 * <p>During plan forking resolution, for each PARTIAL+FINAL pair in a chosen-backend alternative:
 * <ol>
 *   <li>Look up {@link org.opensearch.analytics.spi.AggregateCapability#decomposition()} for
 *       each AggregateCall using the chosen backend.</li>
 *   <li>If null: apply Calcite's {@code AggregateReduceFunctionsRule} to rewrite
 *       AVG → SUM/COUNT, STDDEV → SUM(x²)+SUM(x)+COUNT, etc.</li>
 *   <li>If non-null: use {@link org.opensearch.analytics.spi.AggregateDecomposition#partialCalls()}
 *       to rewrite PARTIAL's aggCalls and output row type, and
 *       {@code AggregateDecomposition.finalExpression()} to
 *       rewrite FINAL's aggCalls. Both must be updated together — the exchange row type
 *       between them must be consistent within the same plan alternative.</li>
 * </ol>
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
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        // Partial aggregate: runs on each partition, keeps input's traits
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.PARTIAL,
            aggregate.getViableBackends()
        );

        // Request SINGLETON distribution — Volcano inserts Exchange automatically
        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        // Build FINAL agg calls. COUNT decomposes to SUM(partial_count) — counting partial
        // rows would give wrong totals. SUM/MIN/MAX are idempotent across phases.
        int groupCardinality = aggregate.getGroupSet().cardinality();
        List<AggregateCall> finalCalls = decomposeForFinal(aggregate, aggregate.getAggCallList(), groupCardinality, partial);

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends()
        );

        // SUM returns nullable BIGINT, but COUNT is non-nullable. Project on top with
        // COALESCE(sum, 0) so the row type matches the original SINGLE aggregate's row
        // type — Volcano's equivSet check rejects nullability differences.
        if (containsCount(aggregate.getAggCallList())) {
            RelNode capped = projectCountsToNotNull(aggregate, finalAggregate, groupCardinality);
            call.transformTo(capped);
        } else {
            call.transformTo(finalAggregate);
        }
    }

    private static boolean containsCount(List<AggregateCall> calls) {
        for (AggregateCall call : calls) {
            if (call.getAggregation().getKind() == SqlKind.COUNT) {
                return true;
            }
        }
        return false;
    }

    /** FINAL agg calls. COUNT → SUM(partial_count) pinned non-nullable BIGINT; SUM/MIN/MAX preserved. */
    private static List<AggregateCall> decomposeForFinal(
        OpenSearchAggregate aggregate,
        List<AggregateCall> partialCalls,
        int groupCardinality,
        RelNode partialInput
    ) {
        List<AggregateCall> finalCalls = new ArrayList<>();
        for (int i = 0; i < partialCalls.size(); i++) {
            AggregateCall pc = partialCalls.get(i);
            int partialOutputCol = groupCardinality + i;
            if (pc.getAggregation().getKind() == SqlKind.COUNT) {
                // SUM has natural type nullable BIGINT — Calcite infers it from partialInput.
                // Nullability is restored by the Project wrapper above the FINAL agg.
                finalCalls.add(
                    AggregateCall.create(
                        SqlStdOperatorTable.SUM,
                        false,
                        false,
                        false,
                        List.of(),
                        List.of(partialOutputCol),
                        -1,
                        null,
                        RelCollations.EMPTY,
                        groupCardinality,
                        partialInput,
                        null,
                        pc.name
                    )
                );
            } else {
                finalCalls.add(pc);
            }
        }
        return finalCalls;
    }

    /**
     * Wraps the FINAL agg with a Project that COALESCEs each COUNT-derived SUM result to 0.
     * Restores the original SINGLE aggregate's row type so Volcano's equivSet check passes.
     */
    private static RelNode projectCountsToNotNull(
        OpenSearchAggregate originalAggregate,
        OpenSearchAggregate finalAggregate,
        int groupCardinality
    ) {
        RexBuilder rexBuilder = originalAggregate.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>();
        for (int g = 0; g < groupCardinality; g++) {
            projects.add(rexBuilder.makeInputRef(finalAggregate, g));
        }
        List<AggregateCall> origCalls = originalAggregate.getAggCallList();
        for (int i = 0; i < origCalls.size(); i++) {
            int colIndex = groupCardinality + i;
            AggregateCall origCall = origCalls.get(i);
            if (origCall.getAggregation().getKind() == SqlKind.COUNT) {
                RelDataType nullableBigint = finalAggregate.getRowType().getFieldList().get(colIndex).getType();
                RexNode sumRef = rexBuilder.makeInputRef(nullableBigint, colIndex);
                RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, originalAggregate.getCluster()
                    .getTypeFactory()
                    .createSqlType(SqlTypeName.BIGINT));
                projects.add(rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, sumRef, zero));
            } else {
                projects.add(rexBuilder.makeInputRef(finalAggregate, colIndex));
            }
        }

        return new OpenSearchProject(
            originalAggregate.getCluster(),
            finalAggregate.getTraitSet(),
            finalAggregate,
            projects,
            originalAggregate.getRowType(),
            originalAggregate.getViableBackends()
        );
    }
}
