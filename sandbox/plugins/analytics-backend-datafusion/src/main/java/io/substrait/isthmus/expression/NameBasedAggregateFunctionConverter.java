/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.substrait.isthmus.expression;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;

/**
 * Aggregate function converter that intercepts Calcite {@code ARG_MIN} / {@code ARG_MAX}
 * calls (PPL's {@code earliest} / {@code latest} lower to these) and rewrites them into
 * substrait {@code first_value} / {@code last_value} invocations with a synthesized sort
 * field. DataFusion 52.x has no native {@code min_by} / {@code max_by} / {@code arg_min}
 * / {@code arg_max} UDAFs, so isthmus's default emission fails to resolve at the
 * substrait consumer.
 */
public class NameBasedAggregateFunctionConverter extends AggregateFunctionConverter {

    private final List<SimpleExtension.AggregateFunctionVariant> allVariants;
    private final TypeConverter typeConverter;

    public NameBasedAggregateFunctionConverter(
        List<SimpleExtension.AggregateFunctionVariant> functions,
        List<FunctionMappings.Sig> additionalSignatures,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter
    ) {
        super(functions, additionalSignatures, typeFactory, typeConverter);
        this.allVariants = List.copyOf(functions);
        this.typeConverter = typeConverter;
    }

    @Override
    public Optional<AggregateFunctionInvocation> convert(
        RelNode input,
        Type.Struct inputType,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        // ARG_MIN / ARG_MAX require substrait-level sort-field synthesis — DataFusion
        // 52.x has no min_by/max_by UDAF; the semantic equivalent is
        // first_value(field) / last_value(field) with an ORDER BY on the key. Handle
        // BEFORE the stock matcher so we can shape the emitted call correctly; see
        // rewriteArgMinMax for details.
        Optional<AggregateFunctionInvocation> argMinMax = rewriteArgMinMax(input, call, topLevelConverter);
        if (argMinMax.isPresent()) {
            return argMinMax;
        }
        return super.convert(input, inputType, call, topLevelConverter);
    }

    /**
     * Rewrites a Calcite {@code ARG_MIN(value, key)} or {@code ARG_MAX(value, key)}
     * aggregate call into a substrait {@code first_value(value)} / {@code last_value(value)}
     * invocation with a synthesized sort field on {@code key}.
     *
     * <p>Background: DataFusion 52.5 ships {@code first_last.rs} only — no native
     * {@code min_by} / {@code max_by} / {@code arg_min} / {@code arg_max} UDAFs.
     * PPL's {@code stats earliest(field, ts)} / {@code latest(field, ts)} lower to
     * {@code SqlStdOperatorTable.ARG_MIN} / {@code ARG_MAX} at the Calcite layer.
     * Without this rewrite, isthmus emits substrait with function name
     * {@code arg_min} / {@code arg_max}, DataFusion's substrait consumer calls
     * {@code FunctionRegistry::udaf(name)}, and misses.
     *
     * <p>Mapping:
     * <ul>
     *   <li>{@code arg_min(value, key)} → {@code first_value(value) ORDER BY key ASC NULLS LAST}</li>
     *   <li>{@code arg_max(value, key)} → {@code last_value(value) ORDER BY key ASC NULLS LAST}</li>
     * </ul>
     *
     * <p>The sort direction is ASC for both; {@code last_value} with ASC returns the
     * max-key row because it's the last row after sorting. Equivalent result to
     * {@code first_value(value) ORDER BY key DESC} and chosen for symmetry with the
     * DataFusion {@code last_value} semantics.
     *
     * <p>Tie-breaking: DataFusion's {@code first_value} / {@code last_value} with an
     * ORDER BY pick the row with the min/max key deterministically, but PPL does not
     * document tie-breaking for earliest/latest. We inherit DataFusion's behavior
     * (whichever row the sort algorithm stabilizes first among equal keys).
     *
     * <p>Returns empty if the call is not ARG_MIN/ARG_MAX, if the arity isn't 2, or
     * if {@code first_value} / {@code last_value} signatures aren't in the loaded
     * extension collection — callers fall through to the stock matcher.
     */
    private Optional<AggregateFunctionInvocation> rewriteArgMinMax(
        RelNode input,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        String callName = call.getAggregation().getName();
        if (callName == null) {
            return Optional.empty();
        }
        boolean isMin = callName.equalsIgnoreCase("ARG_MIN");
        boolean isMax = callName.equalsIgnoreCase("ARG_MAX");
        if (!isMin && !isMax) {
            return Optional.empty();
        }
        if (call.getArgList().size() != 2) {
            return Optional.empty();
        }

        String targetVariantName = isMin ? "first_value" : "last_value";
        SimpleExtension.AggregateFunctionVariant matched = null;
        for (SimpleExtension.AggregateFunctionVariant variant : allVariants) {
            if (variant.name().equalsIgnoreCase(targetVariantName) && variant.requiredArguments().size() == 1) {
                matched = variant;
                break;
            }
        }
        if (matched == null) {
            return Optional.empty();
        }

        int valueIdx = call.getArgList().get(0);
        int keyIdx = call.getArgList().get(1);

        Expression valueExpr = topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, valueIdx));
        Expression keyExpr = topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, keyIdx));

        // ASC NULLS LAST for both: first_value picks the smallest key, last_value picks
        // the largest (rows sorted, first/last row of the sorted set wins).
        Expression.SortField sortField = Expression.SortField.builder()
            .expr(keyExpr)
            .direction(Expression.SortDirection.ASC_NULLS_LAST)
            .build();

        Type outputType = typeConverter.toSubstrait(call.getType());

        Expression.AggregationInvocation invocation = call.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

        return Optional.of(
            ExpressionCreator.aggregateFunction(
                matched,
                outputType,
                Expression.AggregationPhase.INITIAL_TO_RESULT,
                List.of(sortField),
                invocation,
                List.<FunctionArg>of(valueExpr)
            )
        );
    }
}
