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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites single-arg {@code COUNT(DISTINCT x)} and PPL's {@code distinct_count_approx(x)} UDAF
 * marker to {@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT} before the aggregate is marked by
 * {@link OpenSearchAggregateRule}, so substrait dispatch resolves by operator identity. Multi-arg
 * distinct falls through to coordinator-gather in {@link OpenSearchAggregateSplitRule}.
 *
 * <p>Path-awareness: the exact {@code COUNT(DISTINCT x)} → HLL {@code APPROX_COUNT_DISTINCT}
 * rewrite is only correct for the LEGACY Java-DAG path, whose additive PARTIAL/FINAL split cannot
 * merge exact distinct sets across shards. On the DISTRIBUTED (datafusion-distributed) path the
 * whole-query Substrait carries a single logical aggregate and the Rust planner does the
 * partial/final split natively — DataFusion's {@code DistinctCountAccumulator} merges exact
 * set-union state correctly across the {@code NetworkShuffle}. So when {@code rewriteExactCountDistinct}
 * is {@code false} the exact {@code COUNT(DISTINCT x)} is left untouched (isDistinct=true), letting
 * isthmus emit a native {@code count} with the Substrait DISTINCT invocation. The explicit PPL
 * {@code distinct_count_approx} UDF is still lowered to HLL on both paths — the user asked for the
 * approximate function.
 *
 * @opensearch.internal
 */
public class OpenSearchDistinctCountRule extends RelOptRule {

    /**
     * When {@code true} (legacy path), exact single-arg {@code COUNT(DISTINCT x)} is rewritten to
     * HLL {@code APPROX_COUNT_DISTINCT}. When {@code false} (distributed path), it is left as a real
     * {@code COUNT(DISTINCT x)} so DataFusion emits its native exact distinct-count accumulator.
     */
    private final boolean rewriteExactCountDistinct;

    /** Legacy behavior: rewrite exact {@code COUNT(DISTINCT x)} to HLL. */
    public OpenSearchDistinctCountRule() {
        this(true);
    }

    public OpenSearchDistinctCountRule(boolean rewriteExactCountDistinct) {
        super(operand(LogicalAggregate.class, any()), "OpenSearchDistinctCountRule");
        this.rewriteExactCountDistinct = rewriteExactCountDistinct;
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        return agg.getAggCallList().stream().anyMatch(this::needsRewriteToApprox);
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        List<AggregateCall> rewritten = new ArrayList<>(agg.getAggCallList().size());
        boolean changed = false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (needsRewriteToApprox(call)) {
                rewritten.add(rewriteToApprox(call, agg));
                changed = true;
            } else {
                rewritten.add(call);
            }
        }
        if (!changed) return;

        // Widen sub-32-bit integer args to INTEGER so DataFusion uses HLL (Binary state)
        // instead of its bitmap accumulator (List state) which our exchange contract doesn't support.
        RelNode input = widenSmallIntArgs(ruleCall, agg.getInput(), rewritten);

        LogicalAggregate replacement = (LogicalAggregate) agg.copy(
            agg.getTraitSet(),
            input,
            agg.getGroupSet(),
            agg.getGroupSets(),
            rewritten
        );
        // Aggregate.typeMatchesInferred forces the new aggCall to BIGINT NOT NULL while HepPlanner
        // requires the replacement's row type to equal the original's; bridge with a casting Project.
        RelNode rewrittenNode = projectToOriginalRowType(ruleCall, agg, replacement);
        ruleCall.transformTo(rewrittenNode);
    }

    private static RelNode projectToOriginalRowType(RelOptRuleCall ruleCall, LogicalAggregate original, LogicalAggregate replacement) {
        if (replacement.getRowType().equals(original.getRowType())) {
            return replacement;
        }
        RelBuilder relBuilder = ruleCall.builder();
        relBuilder.push(replacement);
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RelDataTypeField> origFields = original.getRowType().getFieldList();
        List<RelDataTypeField> newFields = replacement.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(origFields.size());
        List<String> names = new ArrayList<>(origFields.size());
        for (int i = 0; i < origFields.size(); i++) {
            RexNode ref = rexBuilder.makeInputRef(replacement, i);
            RelDataType targetType = origFields.get(i).getType();
            if (!newFields.get(i).getType().equals(targetType)) {
                ref = rexBuilder.makeCast(targetType, ref);
            }
            projects.add(ref);
            names.add(origFields.get(i).getName());
        }
        relBuilder.project(projects, names, /* forceProject */ true);
        return relBuilder.build();
    }

    /**
     * True when the call must be rewritten to {@code APPROX_COUNT_DISTINCT}. The explicit PPL
     * {@code distinct_count_approx} UDF is always rewritten (the user asked for the approximate
     * function). The exact single-arg {@code COUNT(DISTINCT x)} is only rewritten on the legacy
     * path ({@code rewriteExactCountDistinct}); on the distributed path it is left as a real
     * distinct count for DataFusion's native accumulator.
     */
    private boolean needsRewriteToApprox(AggregateCall call) {
        if (isPplDistinctCountApproxUdf(call)) {
            return true;
        }
        return rewriteExactCountDistinct && isSingleArgCountDistinct(call);
    }

    private static boolean isSingleArgCountDistinct(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.COUNT && call.isDistinct() && call.getArgList().size() == 1;
    }

    /** PPL's distinct_count_approx is a UDF named "APPROX_COUNT_DISTINCT" that is not the stdop. */
    private static boolean isPplDistinctCountApproxUdf(AggregateCall call) {
        return call.getAggregation() != SqlStdOperatorTable.APPROX_COUNT_DISTINCT
            && "APPROX_COUNT_DISTINCT".equals(call.getAggregation().getName())
            && call.getArgList().size() == 1;
    }

    /**
     * If any APPROX_COUNT_DISTINCT arg references a sub-32-bit integer column (TINYINT/SMALLINT),
     * insert a Project that casts those columns to INTEGER. This forces DataFusion to use the
     * HLL accumulator (Binary state) instead of the bitmap accumulator (List state).
     */
    private static RelNode widenSmallIntArgs(RelOptRuleCall ruleCall, RelNode input, List<AggregateCall> calls) {
        List<RelDataTypeField> fields = input.getRowType().getFieldList();
        boolean needsWiden = false;
        for (AggregateCall call : calls) {
            if (call.getAggregation() == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
                for (int argIdx : call.getArgList()) {
                    SqlTypeName typeName = fields.get(argIdx).getType().getSqlTypeName();
                    if (typeName == SqlTypeName.TINYINT || typeName == SqlTypeName.SMALLINT) {
                        needsWiden = true;
                        break;
                    }
                }
            }
            if (needsWiden) break;
        }
        if (!needsWiden) return input;

        RelBuilder builder = ruleCall.builder();
        builder.push(input);
        RexBuilder rexBuilder = builder.getRexBuilder();
        RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        List<RexNode> projects = new ArrayList<>(fields.size());
        List<String> names = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            RexNode ref = rexBuilder.makeInputRef(input, i);
            SqlTypeName typeName = field.getType().getSqlTypeName();
            if (typeName == SqlTypeName.TINYINT || typeName == SqlTypeName.SMALLINT) {
                ref = rexBuilder.makeCast(intType, ref);
            }
            projects.add(ref);
            names.add(field.getName());
        }
        builder.project(projects, names, true);
        return builder.build();
    }

    private static AggregateCall rewriteToApprox(AggregateCall call, LogicalAggregate agg) {
        return AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            /* distinct */ false,
            /* approximate */ false,
            call.ignoreNulls(),
            call.rexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            agg.getGroupSet().cardinality(),
            agg.getInput(),
            /* type */ null,
            call.getName()
        );
    }
}
