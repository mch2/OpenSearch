/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.NameBasedAggregateFunctionConverter;
import io.substrait.isthmus.expression.NameBasedScalarFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage partial aggregate not yet implemented");
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        throw new UnsupportedOperationException("Multi-stage final aggregate not yet implemented");
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("Multi-stage fragment attachment not yet implemented");
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        // PPL `take` isn't in standard Calcite — emit a stub SqlAggFunction whose only
        // job is to seed the converter's name→FunctionFinder map so
        // NameBasedAggregateFunctionConverter can route the actual PPL TAKE operator
        // instance (a different Java object) by case-insensitive name match.
        List<FunctionMappings.Sig> additionalAggSigs = List.of(FunctionMappings.s(stubAgg("take"), "take"));
        AggregateFunctionConverter aggConverter = new NameBasedAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            additionalAggSigs,
            typeFactory,
            typeConverter
        );
        // FunctionMappings.SCALAR_SIGS in substrait-isthmus only registers a partial set of
        // Calcite operators (ABS, SIN, COS, SQRT, CEIL, FLOOR, ROUND, ...). Operators we need
        // that aren't registered there map to nothing in ScalarFunctionConverter's signatures
        // map and trip "Unable to convert call X(...)". Add those mappings explicitly.
        List<FunctionMappings.Sig> additionalScalarSigs = List.of(
            // Logarithmic
            FunctionMappings.s(SqlStdOperatorTable.LN, "ln"),
            FunctionMappings.s(SqlLibraryOperators.LOG2, "log2"),
            FunctionMappings.s(SqlStdOperatorTable.LOG10, "log10"),
            // Trig (TAN missing from SCALAR_SIGS even though SIN/COS are present)
            FunctionMappings.s(SqlStdOperatorTable.TAN, "tan"),
            // Angle conversion
            FunctionMappings.s(SqlStdOperatorTable.DEGREES, "degrees"),
            FunctionMappings.s(SqlStdOperatorTable.RADIANS, "radians"),
            // n-ary comparison
            FunctionMappings.s(SqlLibraryOperators.GREATEST, "greatest"),
            FunctionMappings.s(SqlLibraryOperators.LEAST, "least")
        );
        ScalarFunctionConverter scalarConverter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(),
            additionalScalarSigs,
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

        // substrait-java 0.89+: SubstraitRelVisitor takes a ConverterProvider. The 6-arg
        // constructor lets us plug in our custom scalar/aggregate converters, which is the
        // whole reason we don't adopt the stock `new ConverterProvider(extensions, tf)` form.
        ConverterProvider provider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(provider);
    }

    /**
     * Minimal {@link SqlAggFunction} acting as a name→FunctionFinder map key.
     * PPL emits its own SqlAggFunction instances; identity lookup against these
     * stubs misses, but {@link NameBasedAggregateFunctionConverter} falls back
     * to matching on operator name, which the stub provides.
     */
    private static SqlAggFunction stubAgg(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {};
    }

}
