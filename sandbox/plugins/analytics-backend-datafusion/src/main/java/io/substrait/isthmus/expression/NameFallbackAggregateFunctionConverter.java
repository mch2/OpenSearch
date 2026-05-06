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
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 * Aggregate function converter that adds two behaviors on top of isthmus's stock
 * identity-keyed signature lookup:
 *
 * <ol>
 *   <li><b>Name-based fallback.</b> PPL emits its own {@link org.apache.calcite.sql.SqlAggFunction}
 *       subclass instances not reference-equal to the stub operators seeded in
 *       {@code ADDITIONAL_AGG_SIGS}. When {@link #getFunctionFinder} misses on
 *       identity, we retry by case-insensitive operator name so the PPL call
 *       binds to the right sig.</li>
 *   <li><b>VALUES reshape.</b> PPL {@code values(x)} returns a sorted DISTINCT
 *       list and forces operand coercion to {@code VARCHAR} at the frontend — the
 *       Calcite {@link AggregateCall}'s return type is therefore {@code VARCHAR ARRAY}
 *       even for non-string operands. This cannot be expressed as a Calcite
 *       operator substitution (which would hit {@code typeMatchesInferred}
 *       assertion), so we bypass the stock directMap lookup and assemble the
 *       substrait {@link AggregateFunctionInvocation} directly: rewrite target
 *       name to {@code array_agg}, force DISTINCT, and synthesize an
 *       {@code ORDER BY operand ASC NULLS LAST} sort field.</li>
 * </ol>
 *
 * <p>Other reshapes (ARG_MIN/ARG_MAX) live in per-backend
 * {@link org.opensearch.analytics.spi.BackendCapabilityProvider#aggregateCallAdapters()}
 * applied before fragment conversion — those don't run into the type-mismatch issue
 * because Calcite's {@code FIRST_VALUE}/{@code LAST_VALUE} return types match
 * ARG_MIN/ARG_MAX on the element type.
 *
 * @opensearch.internal
 */
public class NameFallbackAggregateFunctionConverter extends AggregateFunctionConverter {

    private final List<SimpleExtension.AggregateFunctionVariant> allVariants;
    private final TypeConverter typeConverter;

    public NameFallbackAggregateFunctionConverter(
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
    protected FunctionFinder getFunctionFinder(AggregateCall call) {
        FunctionFinder ff = super.getFunctionFinder(call);
        if (ff != null) {
            return ff;
        }
        String name = call.getAggregation().getName();
        if (name == null) {
            return null;
        }
        for (Map.Entry<SqlOperator, FunctionFinder> entry : signatures.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey().getName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Optional<AggregateFunctionInvocation> convert(
        RelNode input,
        Type.Struct inputType,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        // VALUES reshape must run BEFORE the stock matcher — we bypass the directMap
        // lookup entirely and build the invocation ourselves so the output type can
        // be the frontend-declared VARCHAR ARRAY rather than whatever array_agg would
        // infer from the operand type.
        if (isValues(call)) {
            Optional<AggregateFunctionInvocation> reshape = reshapeValues(input, call, topLevelConverter);
            if (reshape.isPresent()) {
                return reshape;
            }
        }
        return super.convert(input, inputType, call, topLevelConverter);
    }

    private static boolean isValues(AggregateCall call) {
        String name = call.getAggregation().getName();
        return name != null && "values".equalsIgnoreCase(name);
    }

    private Optional<AggregateFunctionInvocation> reshapeValues(
        RelNode input,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        if (call.getArgList().size() != 1) {
            return Optional.empty();
        }
        SimpleExtension.AggregateFunctionVariant variant = null;
        for (SimpleExtension.AggregateFunctionVariant v : allVariants) {
            if ("array_agg".equalsIgnoreCase(v.name()) && (v.requiredArguments().size() == 1 || v.args().size() == 1)) {
                variant = v;
                break;
            }
        }
        if (variant == null) {
            return Optional.empty();
        }

        int argIdx = call.getArgList().get(0);
        List<Expression> operands = new ArrayList<>(1);
        operands.add(topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, argIdx)));
        Expression sortExpr = topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, argIdx));
        List<Expression.SortField> sorts = List.of(
            Expression.SortField.builder().expr(sortExpr).direction(Expression.SortDirection.ASC_NULLS_LAST).build()
        );

        Type outputType = typeConverter.toSubstrait(call.getType());

        return Optional.of(
            ExpressionCreator.aggregateFunction(
                variant,
                outputType,
                Expression.AggregationPhase.INITIAL_TO_RESULT,
                sorts,
                Expression.AggregationInvocation.DISTINCT,
                operands.<FunctionArg>stream().toList()
            )
        );
    }

}
