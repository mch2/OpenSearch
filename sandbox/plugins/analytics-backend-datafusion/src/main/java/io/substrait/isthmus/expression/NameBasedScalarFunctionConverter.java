/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.substrait.isthmus.expression;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ToTypeString;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;

/**
 * Scalar function converter with a name-based fallback that picks a variant whose declared
 * arg types match the call's operand types.
 *
 * <p>Two problems this solves that the stock {@link ScalarFunctionConverter} doesn't:
 *
 * <ol>
 *   <li><b>Identity-keyed lookup misses.</b> Stock converter keys its FunctionFinder map by
 *       {@link SqlOperator} identity. PPL ships its own SqlOperator instances (e.g. PPL's
 *       own {@code abs}, {@code sin}); they miss identity lookup against
 *       {@code SqlStdOperatorTable} entries even when the names line up.</li>
 *   <li><b>Calcite-name → DataFusion-registry-name aliasing.</b> Some Calcite operators
 *       resolve to names DataFusion's registry doesn't recognise ({@code SIGN → sign} —
 *       DF calls it {@code signum}; {@code RAND → rand} — DF calls it {@code random};
 *       {@code TRUNCATE → truncate} — DF calls it {@code trunc}). The aliases come from
 *       the {@code calcite_aliases} block in {@code opensearch_scalar.yaml}.</li>
 * </ol>
 *
 * <p>Operand-type coverage (e.g. {@code sin(i64)}) is handled by declaring additional
 * variants in {@code opensearch_scalar.yaml}. Structural rewrites (e.g. dropping
 * LIKE's 3rd escape arg) are handled by
 * {@code AnalyticsSearchBackendPlugin.handleProjectCall} in the planner. By the time a
 * call reaches this converter, its operand types and arity already match a variant in the
 * loaded catalog; this class just finds it by name and, where needed, by alias.
 */
public class NameBasedScalarFunctionConverter extends ScalarFunctionConverter {

    private static final String ALIAS_RESOURCE = "/extensions/opensearch_scalar.yaml";

    /**
     * Calcite operator name (lowercased) → name DataFusion's substrait consumer accepts.
     * Loaded once from {@code opensearch_scalar.yaml}. An entry is only needed when
     * Calcite's operator name doesn't already match what DF recognises.
     */
    static final Map<String, String> ALIAS = loadAliasesFromYaml(ALIAS_RESOURCE);

    /** Resolve the Substrait function name for a Calcite operator name. */
    static String aliasFor(String calciteName) {
        String lower = calciteName.toLowerCase(Locale.ROOT);
        return ALIAS.getOrDefault(lower, lower);
    }

    private static Map<String, String> loadAliasesFromYaml(String resource) {
        try (InputStream in = NameBasedScalarFunctionConverter.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + resource);
            }
            JsonNode root = new ObjectMapper(new YAMLFactory()).readTree(in);
            JsonNode aliases = root.path("calcite_aliases");
            if (!aliases.isObject()) return Map.of();
            Map<String, String> out = new HashMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> it = aliases.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                out.put(e.getKey().toLowerCase(Locale.ROOT), e.getValue().asText());
            }
            return Map.copyOf(out);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load calcite_aliases from " + resource, e);
        }
    }

    private final List<SimpleExtension.ScalarFunctionVariant> allVariants;
    private final TypeConverter typeConverter;

    public NameBasedScalarFunctionConverter(
        List<SimpleExtension.ScalarFunctionVariant> functions,
        List<FunctionMappings.Sig> additionalSignatures,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter
    ) {
        super(functions, additionalSignatures, typeFactory, typeConverter);
        this.allVariants = List.copyOf(functions);
        this.typeConverter = typeConverter;
    }

    @Override
    public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        String lower = call.getOperator().getName().toLowerCase(Locale.ROOT);
        // For aliased operators, force-route through convertByName *before* super.convert —
        // super's directMap would otherwise resolve to the substrait-core variant under the
        // original name (e.g. SIGN → "sign") and ship a plan DataFusion can't resolve.
        if (ALIAS.containsKey(lower)) {
            Optional<Expression> aliased = convertByName(call, topLevelConverter);
            if (aliased.isPresent()) {
                return aliased;
            }
            // fall through to super for safety
        }
        Optional<Expression> result = super.convert(call, topLevelConverter);
        if (result.isPresent()) {
            return result;
        }
        return convertByName(call, topLevelConverter);
    }

    /**
     * Looks up the catalog variant by (aliased) name and exact operand type signature,
     * falling back to first arity-matching variant if no exact-type match exists.
     */
    private Optional<Expression> convertByName(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        String callName = call.getOperator().getName();
        if (callName == null) return Optional.empty();
        String lookup = aliasFor(callName);
        int argCount = call.getOperands().size();

        // Cheap pre-check: do *any* variants match name+arity? If not, return empty before
        // eagerly evaluating operands. This matters because the converter chain runs us before
        // CREATE_SEARCH_CONV, and SEARCH calls carry Sarg literals that LiteralConverter
        // can't handle — we must not visit them when we have nothing to convert.
        boolean nameAndArityMatches = false;
        for (SimpleExtension.ScalarFunctionVariant v : allVariants) {
            if (v.name().equalsIgnoreCase(lookup) && arityFits(v, argCount)) {
                nameAndArityMatches = true;
                break;
            }
        }
        if (!nameAndArityMatches) return Optional.empty();

        List<Expression> operands = call.getOperands().stream().map(topLevelConverter).filter(Objects::nonNull).toList();
        if (operands.size() != argCount) return Optional.empty();
        List<String> operandTypeStrs = operands.stream().map(e -> e.getType().accept(ToTypeString.INSTANCE)).toList();

        SimpleExtension.ScalarFunctionVariant exact = null;
        SimpleExtension.ScalarFunctionVariant arityMatch = null;
        for (SimpleExtension.ScalarFunctionVariant variant : allVariants) {
            if (!variant.name().equalsIgnoreCase(lookup)) continue;
            if (!arityFits(variant, argCount)) continue;
            if (arityMatch == null) arityMatch = variant;
            if (argTypesMatchExactly(variant, operandTypeStrs)) {
                exact = variant;
                break;
            }
        }
        SimpleExtension.ScalarFunctionVariant matched = exact != null ? exact : arityMatch;
        if (matched == null) return Optional.empty();

        Type outputType = typeConverter.toSubstrait(call.getType());
        return Optional.of(
            ExpressionCreator.scalarFunction(matched, outputType, operands.<FunctionArg>stream().toList())
        );
    }

    private static boolean arityFits(SimpleExtension.ScalarFunctionVariant v, int argCount) {
        return v.requiredArguments().size() == argCount || v.args().size() == argCount;
    }

    private static boolean argTypesMatchExactly(
        SimpleExtension.ScalarFunctionVariant v,
        List<String> operandTypeStrs
    ) {
        for (int i = 0; i < operandTypeStrs.size(); i++) {
            int idx = Math.min(i, v.args().size() - 1); // variadic last-arg repeat
            SimpleExtension.Argument arg = v.args().get(idx);
            if (!(arg instanceof SimpleExtension.ValueArgument va)) return false;
            ParameterizedType wantPt = va.value();
            if (wantPt.isWildcard()) continue; // any1/any2 — accept anything
            String wantStr;
            try {
                wantStr = wantPt.accept(ToTypeString.INSTANCE);
            } catch (RuntimeException e) {
                return false;
            }
            if (!wantStr.equals(operandTypeStrs.get(i))) return false;
        }
        return true;
    }

    @Override
    @SuppressWarnings("unused")
    protected Expression generateBinding(
        ScalarFunctionConverter.WrappedScalarCall call,
        SimpleExtension.ScalarFunctionVariant function,
        List<? extends FunctionArg> arguments,
        Type outputType
    ) {
        return super.generateBinding(call, function, arguments, outputType);
    }
}
