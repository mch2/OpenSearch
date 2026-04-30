/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.util.HashSet;
import java.util.Map;
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
public class DataFusionAnalyticsBackendPlugin implements AnalyticsSearchBackendPlugin {

    private static final Set<EngineCapability> ENGINE_CAPS = Set.of(EngineCapability.SORT);

    private static final Set<FieldType> SUPPORTED_FIELD_TYPES = new HashSet<>();
    static {
        SUPPORTED_FIELD_TYPES.addAll(FieldType.numeric());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.keyword());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.date());
        SUPPORTED_FIELD_TYPES.add(FieldType.BOOLEAN);
        SUPPORTED_FIELD_TYPES.add(FieldType.TEXT);
    }

    private static final Set<ScalarFunction> STANDARD_FILTER_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.IN,
        ScalarFunction.LIKE
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM,
        AggregateFunction.SUM0,
        AggregateFunction.MIN,
        AggregateFunction.MAX,
        AggregateFunction.COUNT,
        AggregateFunction.AVG,
        AggregateFunction.FIRST_VALUE,
        AggregateFunction.LAST_VALUE,
        AggregateFunction.FIRST,
        AggregateFunction.LAST,
        AggregateFunction.MEDIAN,
        AggregateFunction.PERCENTILE,
        AggregateFunction.PERCENTILE_CONT,
        AggregateFunction.DISTINCT_COUNT,
        AggregateFunction.DC
    );

    /**
     * Scalar functions DataFusion supports natively. Anything in this set will be
     * routed to DataFusion by {@code OpenSearchProjectRule}; runtime correctness
     * depends on the Substrait core extension catalog already covering it
     * (which is true for the bulk of arithmetic, string, math, and conditional
     * functions). Per-function ITs verify each one.
     */
    private static final Set<ScalarFunction> SCALAR_FUNCTIONS = Set.copyOf(java.util.Arrays.asList(ScalarFunction.values()));

    private static final Map<ScalarFunction, ScalarFunctionAdapter> SCALAR_FUNCTION_ADAPTERS;
    static {
        ScalarFunctionAdapter likeAdapter = new LikeEscapeTransformer();
        ScalarFunctionAdapter timestampAdapter = new TimestampFunctionTransformer();
        ScalarFunctionAdapter divisionAdapter = new SafeDivisionTransformer();
        ScalarFunctionAdapter toNumberAdapter = new ToNumberAdapter();
        ScalarFunctionAdapter toStringAdapter = new ToStringAdapter();
        SCALAR_FUNCTION_ADAPTERS = Map.of(
            ScalarFunction.LIKE, likeAdapter,
            ScalarFunction.TIMESTAMP, timestampAdapter,
            ScalarFunction.DIVIDE, divisionAdapter,
            ScalarFunction.MOD, divisionAdapter,
            ScalarFunction.TONUMBER, toNumberAdapter,
            ScalarFunction.TOSTRING, toStringAdapter
        );
    }

    private final DataFusionPlugin plugin;

    public DataFusionAnalyticsBackendPlugin(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public BackendCapabilityProvider getCapabilityProvider() {
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return ENGINE_CAPS;
            }

            @Override
            public Set<ScanCapability> scanCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                return Set.of(new ScanCapability.DocValues(formats, Set.copyOf(SUPPORTED_FIELD_TYPES)));
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<FilterCapability> caps = new HashSet<>();
                for (ScalarFunction op : STANDARD_FILTER_OPS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(new FilterCapability.Standard(op, Set.of(type), formats));
                    }
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<ProjectCapability> projectCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<ProjectCapability> caps = new HashSet<>();
                for (ScalarFunction func : SCALAR_FUNCTIONS) {
                    caps.add(new ProjectCapability.Scalar(func, Set.copyOf(SUPPORTED_FIELD_TYPES), formats, true));
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<AggregateCapability> caps = new HashSet<>();
                for (AggregateFunction func : AGG_FUNCTIONS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(switch (func.getType()) {
                            case SIMPLE -> AggregateCapability.simple(func, Set.of(type), formats);
                            case STATISTICAL -> AggregateCapability.statistical(func, Set.of(type), formats);
                            case STATE_EXPANDING -> AggregateCapability.stateExpanding(func, Set.of(type), formats);
                            case APPROXIMATE -> AggregateCapability.approximate(func, Set.of(type), formats);
                        });
                    }
                }
                // PPL TAKE — collect first N values into a list. State-expanding (state grows with N).
                caps.add(AggregateCapability.stateExpanding(AggregateFunction.TAKE, Set.copyOf(SUPPORTED_FIELD_TYPES), formats));
                return Set.copyOf(caps);
            }

            @Override
            public Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return SCALAR_FUNCTION_ADAPTERS;
            }
        };
    }

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new DataFusionFragmentConvertor(plugin.getSubstraitExtensions());
    }

    @Override
    public SearchExecEngineProvider getSearchExecEngineProvider() {
        return ctx -> {
            DataFusionService dataFusionService = plugin.getDataFusionService();
            if (dataFusionService == null) {
                throw new IllegalStateException("DataFusionService not initialized — createComponents() may not have been called");
            }

            DatafusionReader dfReader = null;

            if (ctx.getReader() != null) {
                DataFormatRegistry registry = plugin.getDataFormatRegistry();
                for (String formatName : plugin.getSupportedFormats()) {
                    dfReader = ctx.getReader().getReader(registry.format(formatName), DatafusionReader.class);
                    if (dfReader != null) {
                        break;
                    }
                }
            }

            if (dfReader == null) {
                throw new IllegalStateException("No DatafusionReader available in the acquired reader");
            }
            DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, dataFusionService.getNativeRuntime());
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context, dataFusionService::newChildAllocator);
            engine.prepare(ctx);
            return engine;
        };
    }

    @Override
    public ExchangeSinkProvider getExchangeSinkProvider() {
        return ctx -> {
            DataFusionService svc = plugin.getDataFusionService();
            if (svc == null) {
                throw new IllegalStateException("DataFusionService not initialized");
            }
            String mode = plugin.getClusterService() != null
                ? plugin.getClusterService().getClusterSettings().get(DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE)
                : "streaming";
            if ("memtable".equals(mode)) {
                return new DatafusionMemtableReduceSink(ctx, svc.getNativeRuntime());
            }
            return new DatafusionReduceSink(ctx, svc.getNativeRuntime());
        };
    }
}
