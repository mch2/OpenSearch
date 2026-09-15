/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.GetFieldFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;

/**
 * Serializes {@code get_field} to a Substrait {@link Expression.ScalarFunctionInvocation} built
 * directly, rather than letting isthmus match it against a declared signature. Operands are
 * forwarded unchanged — DataFusion's {@code get_field} takes the struct and the field-name literal
 * in that order.
 *
 * <p>The matcher is bypassed because isthmus derives one type every operand must satisfy, and these
 * two are unrelated. The struct operand is the harder half — its type is the object's ROW type, which
 * is data-dependent, so no declaration could name it. The extension declaration is used only as an
 * anchor (name + URN) for the consumer to resolve by name.
 *
 * <p>The output type comes from the Calcite call, which the planner typed from the index mapping.
 * That is what tells the consumer the leaf's type; the declaration's own return type is not consulted.
 *
 * @opensearch.internal
 */
class GetFieldCallConverter implements CallConverter {

    private final SimpleExtension.ExtensionCollection extensions;
    private final TypeConverter typeConverter;

    GetFieldCallConverter(SimpleExtension.ExtensionCollection extensions, TypeConverter typeConverter) {
        this.extensions = extensions;
        this.typeConverter = typeConverter;
    }

    @Override
    public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        if (GetFieldFunction.NAME.equalsIgnoreCase(call.getOperator().getName()) == false) {
            return Optional.empty();
        }

        Optional<SimpleExtension.ScalarFunctionVariant> declaration = findGetFieldDeclaration();
        if (declaration.isEmpty()) {
            // No anchor to reference — decline so the failure surfaces as isthmus' normal
            // "Unable to convert call" rather than an NPE deep in proto serialization.
            return Optional.empty();
        }

        List<FunctionArg> arguments = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            arguments.add(topLevelConverter.apply(operand));
        }

        return Optional.of(
            Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration.get())
                .addAllArguments(arguments)
                .outputType(typeConverter.toSubstrait(call.getType()))
                .build()
        );
    }

    private Optional<SimpleExtension.ScalarFunctionVariant> findGetFieldDeclaration() {
        return extensions.scalarFunctions().stream().filter(variant -> GetFieldFunction.NAME.equals(variant.name())).findFirst();
    }
}
