/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;
import io.substrait.relation.Rel;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * DataFusion fragment converter. Converts stripped Calcite RelNode fragments
 * to Substrait plan bytes that the DataFusion Rust runtime can consume.
 *
 * <p>TODO: implement convertShuffleReadFragment, convertInMemoryFragment, appendShuffleWriter
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private static volatile SimpleExtension.ExtensionCollection EXTENSIONS;

    /**
     * Loads Substrait extensions with the plugin's classloader on the thread context
     * so Jackson's ClassNameIdResolver can find Substrait inner classes
     * (e.g., SimpleExtension$ValueArgument) that live in the plugin classloader,
     * not the server classloader.
     */
    private static SimpleExtension.ExtensionCollection getExtensions() {
        if (EXTENSIONS == null) {
            synchronized (DataFusionFragmentConvertor.class) {
                if (EXTENSIONS == null) {
                    ClassLoader original = Thread.currentThread().getContextClassLoader();
                    try {
                        Thread.currentThread().setContextClassLoader(DataFusionFragmentConvertor.class.getClassLoader());
                        EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION;
                    } finally {
                        Thread.currentThread().setContextClassLoader(original);
                    }
                }
            }
        }
        return EXTENSIONS;
    }

    @Override
    public byte[] convertScanFragment(String tableName, RelNode fragment) {
        LOGGER.info("Converting scan fragment for table [{}]:\n{}", tableName, RelOptUtil.toString(fragment));

        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(field -> field.getValue())
            .toList();

        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        // TODO: The Rust query_executor registers the parquet table using just the index name
        // (e.g. "parquet_simple"), but Calcite produces qualified names with a catalog prefix
        // (e.g. ["opensearch", "parquet_simple"]). Strip the prefix here so the Substrait
        // named_table matches the registered table name. Fix by aligning the table name
        // format between the planner and the execution runtime.
        plan = new TableNameModifier().modifyTableNames(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.info("Substrait plan: {} bytes, proto:\n{}", bytes.length, protoPlan);
        return bytes;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        SimpleExtension.ExtensionCollection extensions = getExtensions();
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(), typeFactory
        );
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(), List.of(), typeFactory, typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(), typeFactory
        );

        return new SubstraitRelVisitor(
            typeFactory, scalarConverter, aggConverter, windowConverter,
            typeConverter, ImmutableFeatureBoard.builder().build()
        );
    }

    /**
     * Strips the Calcite catalog prefix (e.g. "opensearch") from NamedScan table names
     * so DataFusion receives just the index name.
     */
    private static class TableNameModifier {
        Plan modifyTableNames(Plan plan) {
            TableNameVisitor visitor = new TableNameVisitor();
            List<Plan.Root> modifiedRoots = new ArrayList<>();
            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> modifiedRel = root.getInput().accept(visitor, null);
                if (modifiedRel.isPresent()) {
                    modifiedRoots.add(Plan.Root.builder().from(root).input(modifiedRel.get()).build());
                } else {
                    modifiedRoots.add(root);
                }
            }
            return Plan.builder().from(plan).roots(modifiedRoots).build();
        }

        private static class TableNameVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
            @Override
            public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
                List<String> names = namedScan.getNames();
                if (names.size() > 1) {
                    // Keep only the last name (the index name), drop catalog/schema prefixes
                    return Optional.of(
                        NamedScan.builder()
                            .from(namedScan)
                            .names(List.of(names.get(names.size() - 1)))
                            .build()
                    );
                }
                return super.visit(namedScan, context);
            }
        }
    }
}
