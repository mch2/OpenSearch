/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateDecomposition;
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
        SUPPORTED_FIELD_TYPES.addAll(FieldType.text());
        SUPPORTED_FIELD_TYPES.add(FieldType.IP);
        // Container / binary types that appear in real mappings (e.g. the
        // datatypes_nonnumeric fixture used by CalciteDateTimeComparisonIT). The
        // planner's OpenSearchTableScanRule walks every mapped field on the source
        // index and rejects any backend that doesn't declare a capability for each
        // field's type — even when the query never touches those columns. Declaring
        // them here keeps scan planning viable; queries that actually project the
        // column still rely on downstream Arrow/Parquet reader support.
        //
        // Geo types (GEO_POINT / GEO_SHAPE / POINT / SHAPE) intentionally omitted —
        // out of scope for analytics engine; if a test index maps geo fields and
        // the query never touches them, the query should either exclude the backend
        // at planning or the test should be updated to use a non-geo fixture.
        SUPPORTED_FIELD_TYPES.add(FieldType.BINARY);
        SUPPORTED_FIELD_TYPES.add(FieldType.NESTED);
        SUPPORTED_FIELD_TYPES.add(FieldType.OBJECT);
        SUPPORTED_FIELD_TYPES.add(FieldType.FLAT_OBJECT);
        SUPPORTED_FIELD_TYPES.add(FieldType.COMPLETION);
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
        ScalarFunction.LIKE,
        // IP comparison overloads emitted by PPL when an operand is IP-typed.
        // See rust/src/udf/ip_compare.rs for the runtime implementation.
        ScalarFunction.EQUALS_IP,
        ScalarFunction.NOT_EQUALS_IP,
        ScalarFunction.GREATER_IP,
        ScalarFunction.GTE_IP,
        ScalarFunction.LESS_IP,
        ScalarFunction.LTE_IP
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
        AggregateFunction.DC,
        AggregateFunction.APPROX_COUNT_DISTINCT,
        AggregateFunction.STDDEV_POP,
        AggregateFunction.STDDEV_SAMP,
        AggregateFunction.VAR_POP,
        AggregateFunction.VAR_SAMP,
        AggregateFunction.PERCENTILE_APPROX,
        AggregateFunction.APPROX_MEDIAN,
        AggregateFunction.ARG_MIN,
        AggregateFunction.ARG_MAX,
        AggregateFunction.LIST,
        AggregateFunction.VALUES
    );

    /** Per-function partial-state schema declarations. Functions whose state shape
     *  differs from the result type publish here; everything else falls through to
     *  the default (state == result) at the planner layer.
     *
     *  <p>State shapes mirror DataFusion's {@code AggregateUDFImpl::state_fields}
     *  layout for the matching UDAF — DataFusion's {@code AggregateExec(Final)}
     *  reads input columns positionally as state, so the order matters.
     *  AVG → {@code [count BIGINT, sum DOUBLE]} matches DataFusion's
     *  {@code Avg::state_fields} (count first, sum second). */
    /** Welford state: [count BIGINT, mean DOUBLE, m2 DOUBLE]. Shared by all stddev/variance variants. */
    private static final AggregateDecomposition WELFORD_DECOMPOSITION = (original, typeFactory) -> typeFactory.createStructType(
        java.util.List.of(
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT), true),
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE), true),
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE), true)
        ),
        java.util.List.of(original.getName() + "[count]", original.getName() + "[mean]", original.getName() + "[m2]")
    );

    private static final Map<AggregateFunction, AggregateDecomposition> STATE_DECOMPOSITIONS = Map.ofEntries(
        Map.entry(AggregateFunction.AVG, (original, typeFactory) -> typeFactory.createStructType(
            java.util.List.of(
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT), true),
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE), true)
            ),
            java.util.List.of(original.getName() + "[count]", original.getName() + "[sum]")
        )),
        Map.entry(AggregateFunction.APPROX_COUNT_DISTINCT, (original, typeFactory) -> typeFactory.createStructType(
            java.util.List.of(
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARBINARY), true)
            ),
            java.util.List.of(original.getName() + "[sketch]")
        )),
        Map.entry(AggregateFunction.STDDEV_POP, WELFORD_DECOMPOSITION),
        Map.entry(AggregateFunction.STDDEV_SAMP, WELFORD_DECOMPOSITION),
        Map.entry(AggregateFunction.VAR_POP, WELFORD_DECOMPOSITION),
        Map.entry(AggregateFunction.VAR_SAMP, WELFORD_DECOMPOSITION)
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

        SCALAR_FUNCTION_ADAPTERS = Map.ofEntries(
            Map.entry(ScalarFunction.LIKE, likeAdapter),
            Map.entry(ScalarFunction.TIMESTAMP, timestampAdapter),
            Map.entry(ScalarFunction.DIVIDE, divisionAdapter),
            Map.entry(ScalarFunction.MOD, divisionAdapter),
            Map.entry(ScalarFunction.TONUMBER, toNumberAdapter),
            Map.entry(ScalarFunction.TOSTRING, toStringAdapter),
            // YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/DOW/DOY/WEEK/QUARTER/MICROSECOND
            // (and alias siblings MONTH_OF_YEAR/DAYOFMONTH/…) are handled declaratively
            // via opensearch_scalar.yaml calcite_aliases literal-arg injection:
            // year → date_part('year', x), month → date_part('month', x), etc.
            // No DatePartAdapter registration needed.
            // DAYNAME/MONTHNAME are handled declaratively via opensearch_scalar.yaml
            // calcite_aliases literal-arg injection: dayname → to_char(x, 'Day'),
            // monthname → to_char(x, 'Month'). No ToCharAdapter registration needed.
            Map.entry(ScalarFunction.DATEDIFF, new DateDiffAdapter()),
            Map.entry(ScalarFunction.TIMEDIFF, new TimeDiffAdapter()),
            Map.entry(ScalarFunction.TIMESTAMPDIFF, new TimestampDiffAdapter()),
            Map.entry(ScalarFunction.ADDDATE, new DateArithmeticAdapter(true)),
            Map.entry(ScalarFunction.SUBDATE, new DateArithmeticAdapter(false)),
            Map.entry(ScalarFunction.DATE_ADD, new DateArithmeticAdapter(true)),
            Map.entry(ScalarFunction.DATE_SUB, new DateArithmeticAdapter(false)),
            Map.entry(ScalarFunction.WEEKDAY, new WeekdayAdapter()),
            Map.entry(ScalarFunction.YEARWEEK, new YearweekAdapter()),
            Map.entry(ScalarFunction.MINUTE_OF_DAY, new MinuteOfDayAdapter()),
            Map.entry(ScalarFunction.SEC_TO_TIME, new SecToTimeAdapter()),
            Map.entry(ScalarFunction.TIME_TO_SEC, new TimeToSecAdapter()),
            // SpanAdapter removed: PPL visitor emits OPENSEARCH_SPAN (Rust UDF) directly,
            // never ScalarFunction.SPAN, so the adapter never fired. See
            // sandbox-backend-datafusion/rust/src/udf/opensearch_span.rs.
            // SPAN_BUCKET: rust UDF span_bucket (see rust/src/udf/span_bucket.rs) —
            // bit-exact with sql-repo SpanBucketFunction (integer/float formatting).
            // WIDTH_BUCKET: rust UDF width_bucket (see rust/src/udf/width_bucket.rs) —
            // bit-exact with sql-repo WidthBucketFunction (maxValue%width rescale).
            Map.entry(ScalarFunction.TIMESTAMPADD, new TimestampAddAdapter()),
            // PPL aliases num/number_to_string for tonumber/tostring — same CAST rewrite.
            Map.entry(ScalarFunction.NUM, toNumberAdapter),
            Map.entry(ScalarFunction.NUMBER_TO_STRING, toStringAdapter),
            // PPL strcmp(a,b) → CASE compare. RMCOMMA/RMUNIT are handled declaratively
            // via opensearch_scalar.yaml calcite_aliases literal-arg injection:
            // rmcomma → regexp_replace(s, ',', ''), rmunit → regexp_replace(s, '[A-Za-z]+$', '').
            Map.entry(ScalarFunction.STRCMP, new StrcmpAdapter()),
            // IP cast: strip the IP(string) wrapper since the Rust ip_compare
            // UDFs accept Utf8 operands directly.
            Map.entry(ScalarFunction.IP, new IpCastAdapter()),
            // Calcite ITEM on an array operand → DataFusion array_element. Struct-typed
            // ITEM passes through (the adapter no-ops for non-ARRAY operand[0]).
            Map.entry(ScalarFunction.ITEM, new ItemArrayElementAdapter()),
            // LAST_DAY: rust UDF last_day (see rust/src/udf/last_day.rs) — direct
            // chrono month-add-and-subtract-one-day. No adapter needed.
            Map.entry(ScalarFunction.FROM_DAYS, new FromDaysAdapter()),
            Map.entry(ScalarFunction.TO_DAYS, new ToDaysAdapter()),
            Map.entry(ScalarFunction.TO_SECONDS, new ToSecondsAdapter()),
            Map.entry(ScalarFunction.PERIOD_ADD, new PeriodAddAdapter()),
            Map.entry(ScalarFunction.PERIOD_DIFF, new PeriodDiffAdapter()),
            Map.entry(ScalarFunction.GET_FORMAT, new GetFormatAdapter()),
            // CONVERT_TZ: rust UDF convert_tz (see rust/src/udf/convert_tz.rs) —
            // DST-correct wall-clock conversion via chrono-tz. No adapter needed;
            // the name-based converter routes by YAML signature.
            // ILIKE: substrait has no case-insensitive `like`; fold to LIKE(LOWER(a), LOWER(b)).
            Map.entry(ScalarFunction.ILIKE, new IlikeAdapter()),
            // ARRAY_COMPACT: DataFusion 52.5.0 has no array_compact but has array_remove_all.
            // Rewrite array_compact(x) → array_remove_all(x, CAST(NULL AS T)) where T is x's
            // element type. Paired with the array_remove_all signature in opensearch_scalar.yaml.
            Map.entry(ScalarFunction.ARRAY_COMPACT, new ArrayCompactAdapter())
            // REX_EXTRACT: rust UDF rex_extract (see rust/src/udf/rex_extract.rs) —
            // runtime named-group resolution via regex crate. No adapter needed;
            // replaces RexExtractAdapter which required literal pattern + group.
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
                    AggregateDecomposition decomposition = STATE_DECOMPOSITIONS.get(func);
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(new AggregateCapability(func, Set.of(type), formats, decomposition));
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
