/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.analytics.spi.StdOperatorRewriteAdapter;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.analytics.spi.WindowFunctionCapability;
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

    private static final Set<EngineCapability> ENGINE_CAPS = Set.of(EngineCapability.SORT, EngineCapability.UNION);

    private static final Set<FieldType> SUPPORTED_FIELD_TYPES = new HashSet<>();
    static {
        SUPPORTED_FIELD_TYPES.addAll(FieldType.numeric());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.keyword());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.date());
        SUPPORTED_FIELD_TYPES.add(FieldType.BOOLEAN);
        SUPPORTED_FIELD_TYPES.add(FieldType.TEXT);
    }

    // Filter-side scalar functions DataFusion can evaluate natively. Comparisons, arithmetic
    // (for `where x + y > 0`-style predicates), and Calcite's SARG fold (IN/BETWEEN/range-union)
    // are all supported via the Substrait default extension catalog. AND/OR/NOT are recursed into
    // by {@link OpenSearchFilterRule} structurally and never looked up here, but registering them
    // keeps the capability declaration complete for auditing and symmetric with PROJECT_OPS.
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
        ScalarFunction.LIKE,
        ScalarFunction.SARG_PREDICATE,
        ScalarFunction.PLUS,
        ScalarFunction.MINUS,
        ScalarFunction.TIMES,
        ScalarFunction.DIVIDE,
        ScalarFunction.MOD
    );

    // Project-side scalar functions DataFusion can evaluate natively. Each entry corresponds to a
    // PPL command/function we want the analytics-engine planner to route through DataFusion. Add
    // here only after verifying the function deserializes through Substrait isthmus into a plan
    // DataFusion's native runtime can execute (see DataFusionFragmentConvertor for the conversion
    // path). COALESCE is the lowering target of PPL `fillnull`. CAST is required because
    // ReduceExpressionsRule.ProjectReduceExpressionsRule (in PlannerImpl) constant-folds field
    // references through equality filters into typed literals — e.g. after `where str0 = 'FURNITURE'`,
    // the projection `fields str0` is rewritten to `CAST('FURNITURE' AS VARCHAR)`. The remaining
    // comparison / arithmetic / logical operators are project-capable for eval-style projections.
    private static final Set<ScalarFunction> STANDARD_PROJECT_OPS = Set.of(
        ScalarFunction.CASE,
        ScalarFunction.COALESCE,
        ScalarFunction.CEIL,
        ScalarFunction.CAST,
        ScalarFunction.SARG_PREDICATE,
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IN,
        ScalarFunction.LIKE,
        ScalarFunction.PLUS,
        ScalarFunction.MINUS,
        ScalarFunction.TIMES,
        ScalarFunction.DIVIDE,
        ScalarFunction.MOD,
        ScalarFunction.YEAR,
        ScalarFunction.CONVERT_TZ,
        ScalarFunction.UNIX_TIMESTAMP
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM,
        AggregateFunction.SUM0,
        AggregateFunction.MIN,
        AggregateFunction.MAX,
        AggregateFunction.COUNT,
        AggregateFunction.AVG
    );

    // Window functions declared for streamstats / eventstats / trendline coverage. Each entry
    // goes through isthmus's WindowFunctionConverter (already wired in DataFusionFragmentConvertor)
    // and DataFusion's native substrait consumer — no new conversion path needed per function.
    //
    // Aggregate-style window functions (SUM/COUNT/MIN/MAX/AVG) are used by streamstats with the
    // matching aggregator, and by trendline sma which lowers to AVG OVER ROWS N-1 PRECEDING +
    // COUNT OVER for the warm-up CASE. Ranking / navigation functions (ROW_NUMBER/RANK/
    // DENSE_RANK/NTH_VALUE) cover streamstats's ranking forms and trendline wma's NTH_VALUE
    // expansion. LEAD/LAG/NTILE are declared even without a current PPL command using them —
    // their substrait conversion is identical and declaring them here keeps the capability
    // surface aligned with the WindowFunction enum.
    private static final Set<WindowFunction> WINDOW_FUNCTIONS = Set.of(
        WindowFunction.SUM,
        WindowFunction.COUNT,
        WindowFunction.MIN,
        WindowFunction.MAX,
        WindowFunction.AVG,
        WindowFunction.ROW_NUMBER,
        WindowFunction.RANK,
        WindowFunction.DENSE_RANK,
        WindowFunction.NTH_VALUE,
        WindowFunction.LEAD,
        WindowFunction.LAG,
        WindowFunction.NTILE
    );

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
                for (ScalarFunction op : STANDARD_PROJECT_OPS) {
                    caps.add(new ProjectCapability.Scalar(op, Set.copyOf(SUPPORTED_FIELD_TYPES), formats, true));
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<AggregateCapability> caps = new HashSet<>();
                for (AggregateFunction func : AGG_FUNCTIONS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(AggregateCapability.simple(func, Set.of(type), formats));
                    }
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<WindowFunctionCapability> windowFunctionCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<WindowFunctionCapability> caps = new HashSet<>();
                for (WindowFunction func : WINDOW_FUNCTIONS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(new WindowFunctionCapability(func, Set.of(type), formats));
                    }
                }
                return Set.copyOf(caps);
            }

            @Override
            public Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return Map.ofEntries(
                    Map.entry(ScalarFunction.TIMESTAMP, new TimestampFunctionAdapter()),
                    Map.entry(ScalarFunction.SARG_PREDICATE, new SargAdapter()),
                    Map.entry(ScalarFunction.DIVIDE, new StdOperatorRewriteAdapter("DIVIDE", SqlStdOperatorTable.DIVIDE)),
                    Map.entry(ScalarFunction.MOD, new StdOperatorRewriteAdapter("MOD", SqlStdOperatorTable.MOD)),
                    Map.entry(ScalarFunction.LIKE, new LikeAdapter()),
                    Map.entry(ScalarFunction.YEAR, new YearAdapter()),
                    Map.entry(ScalarFunction.CONVERT_TZ, new ConvertTzAdapter()),
                    Map.entry(ScalarFunction.UNIX_TIMESTAMP, new UnixTimestampAdapter())
                );
            }
        };
    }

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new DataFusionFragmentConvertor(plugin.getSubstraitExtensions());
    }

    @Override
    public SearchExecEngineProvider getSearchExecEngineProvider() {
        return (ctx, backendContext) -> {
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
            if (backendContext != null) {
                DataFusionSessionState sessionState = (DataFusionSessionState) backendContext;
                context.setSessionContextHandle(sessionState.sessionContextHandle());
            }
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context);
            engine.prepare(ctx);
            return engine;
        };
    }

    @Override
    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return new DataFusionInstructionHandlerFactory(plugin);
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
            // Memtable mode is single-input only (DatafusionMemtableReduceSink registers
            // exactly one MemTable at close time). Multi-input shapes (Union, future Join)
            // need per-child input partitions, which only the streaming sink implements via
            // MultiInputExchangeSink#sinkForChild. Auto-fall-back to streaming so end users
            // don't have to flip the cluster setting per query.
            // TODO: lift this fallback once the memtable sink registers one MemTable per
            // child stage (see DatafusionMemtableReduceSink class javadoc).
            if ("memtable".equals(mode) && ctx.childInputs().size() == 1) {
                return new DatafusionMemtableReduceSink(ctx, svc.getNativeRuntime());
            }
            return new DatafusionReduceSink(ctx, svc.getNativeRuntime());
        };
    }
}
