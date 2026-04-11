/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SPI extension discovered by analytics-engine via {@code META-INF/services}.
 * <p>
 * Receives the fully-initialized {@link DataFusionPlugin} instance via its single-arg
 * constructor (supported by {@code PluginsService.createExtension()}), so it has access
 * to the {@link DataFusionService} created during plugin lifecycle.
 * <p>
 * Declares all analytics query capabilities (operators, filters, aggregates) and
 * creates per-shard execution engines.
 */
public class DataFusionAnalyticsExtension implements AnalyticsSearchBackendPlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionAnalyticsExtension.class);

    private static final Set<OperatorCapability> OPERATOR_CAPS = Set.of(
        OperatorCapability.SCAN,
        OperatorCapability.FILTER,
        OperatorCapability.AGGREGATE,
        OperatorCapability.SORT,
        OperatorCapability.PROJECT,
        OperatorCapability.COORDINATOR_REDUCE
    );

    private static final Set<OperatorCapability> ARROW_COMPATIBLE_OPS = Set.of(
        OperatorCapability.FILTER,
        OperatorCapability.AGGREGATE,
        OperatorCapability.SORT,
        OperatorCapability.PROJECT
    );

    private static final Set<FieldType> SUPPORTED_FIELD_TYPES = new HashSet<>();
    static {
        SUPPORTED_FIELD_TYPES.addAll(FieldType.numeric());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.keyword());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.date());
        SUPPORTED_FIELD_TYPES.add(FieldType.BOOLEAN);
    }

    private static final Set<FilterOperator> STANDARD_FILTER_OPS = Set.of(
        FilterOperator.EQUALS,
        FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN,
        FilterOperator.GREATER_THAN_OR_EQUAL,
        FilterOperator.LESS_THAN,
        FilterOperator.LESS_THAN_OR_EQUAL,
        FilterOperator.IS_NULL,
        FilterOperator.IS_NOT_NULL,
        FilterOperator.IN,
        FilterOperator.LIKE
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM,
        AggregateFunction.SUM0,
        AggregateFunction.MIN,
        AggregateFunction.MAX,
        AggregateFunction.COUNT,
        AggregateFunction.AVG
    );

    private final DataFusionPlugin plugin;

    public DataFusionAnalyticsExtension(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public Set<OperatorCapability> supportedOperators() {
        return OPERATOR_CAPS;
    }

    @Override
    public Set<FilterCapability> filterCapabilities() {
        Set<String> formats = DataFusionPlugin.getSupportedFormatNames();
        Set<FilterCapability> caps = new HashSet<>();
        for (FilterOperator op : STANDARD_FILTER_OPS) {
            for (FieldType type : SUPPORTED_FIELD_TYPES) {
                caps.add(new FilterCapability.Standard(op, type, formats));
            }
        }
        return Set.copyOf(caps);
    }

    @Override
    public Set<AggregateCapability> aggregateCapabilities() {
        Set<String> formats = DataFusionPlugin.getSupportedFormatNames();
        Set<AggregateCapability> caps = new HashSet<>();
        for (AggregateFunction func : AGG_FUNCTIONS) {
            for (FieldType type : SUPPORTED_FIELD_TYPES) {
                caps.add(AggregateCapability.simple(func, type, formats));
            }
        }
        return Set.copyOf(caps);
    }

    @Override
    public Set<OperatorCapability> arrowCompatibleOperators() {
        return ARROW_COMPATIBLE_OPS;
    }

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new DataFusionFragmentConvertor();
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        DataFusionService dataFusionService = plugin.getDataFusionService();
        if (dataFusionService == null) {
            throw new IllegalStateException("DataFusionService not initialized — createComponents() may not have been called");
        }

        DatafusionReader dfReader = null;

        // Get reader from the execution context (via DataFormatAwareEngine → DatafusionReaderManager)
        if (ctx.getReader() != null) {
            List<DataFormat> formats = plugin.getSupportedFormats();
            if (formats != null) {
                for (DataFormat format : formats) {
                    dfReader = ctx.getReader().getReader(format, DatafusionReader.class);
                    if (dfReader != null) {
                        break;
                    }
                }
            }
        }

        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available — index may not have been refreshed yet");
        }
        DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, dataFusionService.getNativeRuntime());
        DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context, dataFusionService::newChildAllocator);
        engine.prepare(ctx);
        return engine;
    }
}
