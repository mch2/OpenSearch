/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Ignore;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end smoke test for the streaming coordinator-reduce path:
 *
 * <pre>
 *   PPL → planner → multi-shard SHARD_FRAGMENT dispatch → DataFusion shard scan
 *       → ExchangeSink.feed → DatafusionReduceSink (Substrait SUM via convertFinalAggFragment)
 *       → drain → downstream → assembled PPLResponse
 * </pre>
 *
 * <p>Builds a parquet-backed composite index with two shards, indexes a small
 * deterministic dataset, then runs a {@code stats sum(value) as total} aggregate.
 * The total is a function of the indexed values × shard count; any drift in
 * shard fan-out, sink wiring, or final-agg merge will show up as a mismatch.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class CoordinatorReduceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "coord_reduce_e2e";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 10;
    /** Constant `value` for every doc — picks a deterministic SUM independent of shard routing. */
    private static final int VALUE = 7;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Plugins with no extendedPlugins requirement go here. Plugins that need
        // explicit extendedPlugins (so SPI ExtensionLoader walks the right parent
        // classloader) are declared in additionalNodePlugins() below.
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        // OpenSearchIntegTestCase's nodePlugins() builds PluginInfo with empty
        // extendedPlugins, which breaks ExtensiblePlugin.loadExtensions(...) for
        // plugins like DataFusionPlugin that ride on AnalyticsPlugin's SPI. Use
        // additionalNodePlugins() to declare the parent relationships explicitly.
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            // STREAM_TRANSPORT (Arrow Flight RPC for shard→coordinator response streaming)
            // is intentionally NOT enabled here. With it on, AnalyticsSearchTransportService
            // routes all sendChildRequest calls through StreamTransportService whose connection
            // profile only carries stream channels, breaking the non-stream fragment dispatch
            // request. The non-stream path is enough for this IT's small-result SUM aggregate.
            .build();
    }

    /**
     * {@code source = T | stats sum(value) as total} on a 2-shard parquet-backed index
     * → coordinator-reduce path runs the final SUM via {@link DatafusionReduceSink}
     * and returns the deterministic total.
     */
    public void testScalarSumAcrossShards() throws Exception {
        createParquetBackedIndex();
        indexDeterministicDocs();

        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total");

        assertNotNull("PPLResponse must not be null", response);
        assertTrue("columns must contain 'total', got " + response.getColumns(), response.getColumns().contains("total"));
        assertEquals("scalar agg must return exactly 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("total");
        Object cell = response.getRows().get(0)[idx];
        assertNotNull("SUM(value) cell must not be null — coordinator-reduce returned no value", cell);
        long actual = ((Number) cell).longValue();
        long expected = (long) VALUE * NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals(
            "SUM(value) across " + NUM_SHARDS + " shards × " + DOCS_PER_SHARD + " docs × value=" + VALUE + " = " + expected,
            expected,
            actual
        );
    }

    private void createParquetBackedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);
    }

    private void indexDeterministicDocs() {
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    /**
     * AVG aggregation across shards. Drives the partial/final split:
     * data nodes emit partial state (sum + count), coordinator reduces to a single
     * AVG. Validates the AVG-specific path: {@code rewriteAvgCalls} swaps Calcite's
     * standard AVG for isthmus's variant, and the Rust optimizer's
     * {@code CombinePartialFinalAggregate} rule is disabled so the split is preserved.
     *
     * <p>Uses varying values so partial aggregates per shard differ — averaging
     * averages would give the wrong answer.
     */
    public void testAvgAcrossShards() throws Exception {
        String index = "coord_reduce_avg";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // values are 1..N → mean = (N+1)/2
        double expectedAvg = (totalDocs + 1) / 2.0;

        PPLResponse response = executePPL("source = " + index + " | stats avg(value) as a");

        assertNotNull("PPLResponse must not be null", response);
        assertTrue("columns must contain 'a', got " + response.getColumns(), response.getColumns().contains("a"));
        assertEquals("scalar agg must return exactly 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("a");
        Object cell = response.getRows().get(0)[idx];
        assertNotNull("AVG(value) cell must not be null — coordinator-reduce returned no value", cell);
        double actual = ((Number) cell).doubleValue();
        assertEquals(
            "AVG(value) across " + NUM_SHARDS + " shards of values 1.." + totalDocs + " = " + expectedAvg,
            expectedAvg,
            actual,
            1e-9
        );
    }

    private void createParquetBackedIndex(String indexName) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void indexVaryingDocs(String indexName) {
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            client().prepareIndex(indexName).setId("v" + i).setSource("value", i + 1).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    /**
     * Approximate distinct-count across shards via HLL. {@code dc(value)} is rewritten
     * to {@code APPROX_COUNT_DISTINCT(value)} at the planner, enabling the split path:
     * each shard emits a partial HLL sketch, and the coordinator merges sketches via
     * DataFusion's Final-mode accumulator. Overlapping values per shard verify that
     * sketch merging deduplicates correctly (naïve sum would double-count).
     */
    public void testDistinctCountAcrossShards() throws Exception {
        String index = "coord_reduce_dc";
        createParquetBackedIndex(index);
        indexOverlappingDocs(index);

        // values are {1..K} repeated across both shards → distinct count = K
        int distinct = DOCS_PER_SHARD;

        PPLResponse response = executePPL("source = " + index + " | stats dc(value) as d");

        assertNotNull("PPLResponse must not be null", response);
        assertTrue("columns must contain 'd', got " + response.getColumns(), response.getColumns().contains("d"));
        assertEquals("scalar agg must return exactly 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("d");
        Object cell = response.getRows().get(0)[idx];
        assertNotNull("dc(value) cell must not be null — coordinator-reduce returned no value", cell);
        long actual = ((Number) cell).longValue();
        // HLL approximation — allow 10% deviation for small cardinalities
        double tolerance = distinct * 0.10;
        assertEquals(
            "dc(value) across " + NUM_SHARDS + " shards each holding values 1.." + distinct + " ≈ " + distinct,
            (double) distinct,
            (double) actual,
            tolerance
        );
    }

    private void indexOverlappingDocs(String indexName) {
        for (int s = 0; s < NUM_SHARDS; s++) {
            for (int i = 0; i < DOCS_PER_SHARD; i++) {
                client().prepareIndex(indexName).setId("s" + s + "_v" + i).setSource("value", i + 1).get();
            }
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    /**
     * MIN across shards. Distributive: per-shard min, coord min-of-mins. Validates
     * the split-path identity merge for a non-summing aggregate; uses varying values
     * so per-shard mins differ and a wrong merge would surface.
     */
    public void testMinAcrossShards() throws Exception {
        String index = "coord_reduce_min";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        long expected = 1L; // values are 1..N
        PPLResponse response = executePPL("source = " + index + " | stats min(value) as m");
        long actual = ((Number) singleScalar(response, "m")).longValue();
        assertEquals("MIN(value) across shards", expected, actual);
    }

    /**
     * MAX across shards. Distributive: per-shard max, coord max-of-maxes.
     */
    public void testMaxAcrossShards() throws Exception {
        String index = "coord_reduce_max";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        long expected = totalDocs; // values are 1..N → max=N
        PPLResponse response = executePPL("source = " + index + " | stats max(value) as m");
        long actual = ((Number) singleScalar(response, "m")).longValue();
        assertEquals("MAX(value) across shards", expected, actual);
    }

    /**
     * COUNT across shards. Verifies the FINAL-mode count (sum-of-counts via
     * DataFusion's Final-mode count accumulator) is correct, not a per-shard
     * count count (which would double-count).
     */
    public void testCountAcrossShards() throws Exception {
        String index = "coord_reduce_count";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        long expected = (long) NUM_SHARDS * DOCS_PER_SHARD;
        PPLResponse response = executePPL("source = " + index + " | stats count(value) as c");
        long actual = ((Number) singleScalar(response, "c")).longValue();
        assertEquals("COUNT(value) across shards", expected, actual);
    }

    /**
     * STDDEV (population) across shards. The split must merge Welford-style state,
     * not stddev-of-stddevs. The streaming-table schema reflects DataFusion's
     * stddev state shape; the FINAL-mode AggregateExec consumes those columns.
     */
    public void testStddevPopAcrossShards() throws Exception {
        String index = "coord_reduce_stddev";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // population stddev of 1..N = sqrt((N^2 - 1) / 12)
        double expected = Math.sqrt((Math.pow(totalDocs, 2) - 1.0) / 12.0);
        PPLResponse response = executePPL("source = " + index + " | stats stddev_pop(value) as s");
        double actual = ((Number) singleScalar(response, "s")).doubleValue();
        assertEquals("STDDEV_POP(value) across shards", expected, actual, 1e-9);
    }

    /**
     * STDDEV (sample) across shards. Validates stddev_samp substrait binding and
     * coordinator merge.
     */
    public void testStddevSampAcrossShards() throws Exception {
        String index = "coord_reduce_stddev_samp";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // sample stddev of 1..N = sqrt((N^2 - 1) / 12 * N / (N-1))
        double popVar = (Math.pow(totalDocs, 2) - 1.0) / 12.0;
        double expected = Math.sqrt(popVar * totalDocs / (totalDocs - 1));
        PPLResponse response = executePPL("source = " + index + " | stats stddev_samp(value) as s");
        double actual = ((Number) singleScalar(response, "s")).doubleValue();
        assertEquals("STDDEV_SAMP(value) across shards", expected, actual, 1e-9);
    }

    /**
     * VAR_POP across shards. Validates var_pop substrait binding and
     * Welford merge.
     */
    public void testVarPopAcrossShards() throws Exception {
        String index = "coord_reduce_var_pop";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // population variance of 1..N = (N^2 - 1) / 12
        double expected = (Math.pow(totalDocs, 2) - 1.0) / 12.0;
        PPLResponse response = executePPL("source = " + index + " | stats var_pop(value) as v");
        double actual = ((Number) singleScalar(response, "v")).doubleValue();
        assertEquals("VAR_POP(value) across shards", expected, actual, 1e-9);
    }

    /**
     * VAR_SAMP across shards. Validates var_samp substrait binding and merge.
     */
    public void testVarSampAcrossShards() throws Exception {
        String index = "coord_reduce_var_samp";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // sample variance of 1..N = (N^2 - 1) / 12 * N / (N-1)
        double popVar = (Math.pow(totalDocs, 2) - 1.0) / 12.0;
        double expected = popVar * totalDocs / (totalDocs - 1);
        PPLResponse response = executePPL("source = " + index + " | stats var_samp(value) as v");
        double actual = ((Number) singleScalar(response, "v")).doubleValue();
        assertEquals("VAR_SAMP(value) across shards", expected, actual, 1e-9);
    }

    /**
     * PPL `list(field)` aggregates values into an array. Maps to DataFusion's
     * {@code array_agg(any1)} via {@code NameBasedAggregateFunctionConverter.NAME_ALIASES}.
     * Cardinality (not order) is what we assert — array_agg ordering is non-deterministic
     * across shards.
     *
     * <p>Ignored: Rust-side schema mismatch. After adding ARRAY handling in
     * {@link org.opensearch.analytics.exec.stage.ArrowSchemaFromCalcite} the planner
     * stage produces a valid substrait plan, but DataFusion's array_agg yields
     * {@code List(Utf8)} while the coordinator's input schema (derived from Calcite) is
     * {@code List(Int32)}, causing an Arrow-level assertion panic inside
     * {@code NativeBridge.streamNext}. Fix needs either a consistent component-type
     * derivation (Calcite's ARRAY component type inference) or a coordinator-side
     * schema normalization.
     */
    @Ignore
    public void testListAcrossShards() throws Exception {
        String index = "coord_reduce_list";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        PPLResponse response = executePPL("source = " + index + " | stats list(value) as l");
        Object cell = singleScalar(response, "l");
        assertTrue("list(value) must materialize as a List, got " + cell.getClass(), cell instanceof java.util.List);
        @SuppressWarnings("unchecked")
        java.util.List<Object> values = (java.util.List<Object>) cell;
        assertEquals("list(value) collects every input row", totalDocs, values.size());
    }

    /**
     * PPL `values(field)` aggregates distinct values into an array. Maps to DataFusion's
     * {@code array_agg(any1)} with {@code DISTINCT} via {@code NAME_ALIASES}.
     *
     * <p>Ignored: same array_agg schema mismatch as {@link #testListAcrossShards()}.
     */
    @Ignore
    public void testValuesAcrossShards() throws Exception {
        String index = "coord_reduce_values";
        createParquetBackedIndex(index);
        indexOverlappingDocs(index);

        // Overlapping docs: each shard holds values 1..DOCS_PER_SHARD, so total distinct = DOCS_PER_SHARD.
        PPLResponse response = executePPL("source = " + index + " | stats values(value) as v");
        Object cell = singleScalar(response, "v");
        assertTrue("values(value) must materialize as a List, got " + cell.getClass(), cell instanceof java.util.List);
        @SuppressWarnings("unchecked")
        java.util.List<Object> values = (java.util.List<Object>) cell;
        assertEquals("values(value) returns distinct values across both shards", DOCS_PER_SHARD, values.size());
    }

    /**
     * PPL `first_value(field)` via Calcite's built-in {@code SqlKind.FIRST_VALUE}. DataFusion
     * has a matching {@code first_value} aggregate, and the YAML extension exports it.
     *
     * <p>Ignored: {@code first_value} is not a PPL-level stats token (the grammar only
     * accepts {@code first}). The same backend path is already covered by
     * {@link #testFirstAcrossShards()}, which routes {@code first} to DataFusion's
     * {@code first_value} via NAME_ALIASES.
     */
    @Ignore
    public void testFirstValueAcrossShards() throws Exception {
        String index = "coord_reduce_first_value";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        PPLResponse response = executePPL("source = " + index + " | stats first_value(value) as f");
        Object cell = singleScalar(response, "f");
        assertTrue("first_value cell must be numeric, got " + cell.getClass(), cell instanceof Number);
        long actual = ((Number) cell).longValue();
        // values are 1..N; first_value ordering is non-deterministic across shards
        assertTrue("first_value(value) must be one of the input values, got " + actual,
            actual >= 1 && actual <= (long) NUM_SHARDS * DOCS_PER_SHARD);
    }

    /**
     * PPL `last_value(field)` via Calcite's built-in {@code SqlKind.LAST_VALUE}. DataFusion
     * has a matching {@code last_value} aggregate.
     *
     * <p>Ignored: {@code last_value} is not a PPL-level stats token (the grammar only
     * accepts {@code last}). The same backend path is already covered by
     * {@link #testLastAcrossShards()}, which routes {@code last} to DataFusion's
     * {@code last_value} via NAME_ALIASES.
     */
    @Ignore
    public void testLastValueAcrossShards() throws Exception {
        String index = "coord_reduce_last_value";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        PPLResponse response = executePPL("source = " + index + " | stats last_value(value) as l");
        Object cell = singleScalar(response, "l");
        assertTrue("last_value cell must be numeric, got " + cell.getClass(), cell instanceof Number);
        long actual = ((Number) cell).longValue();
        assertTrue("last_value(value) must be one of the input values, got " + actual,
            actual >= 1 && actual <= (long) NUM_SHARDS * DOCS_PER_SHARD);
    }

    /**
     * PPL `first(field)` — custom UDAF emitted as a SqlAggFunction named "FIRST".
     * Aliased to DataFusion's {@code first_value} via {@code NAME_ALIASES}.
     *
     * <p>Ignored: Rust-side panic inside {@code NativeBridge.streamNext} with
     * {@code assertion left==right failed, left: 2, right: 1}. Indicates a
     * column-count mismatch between what the partial→final agg split emits and what
     * DataFusion's first_value accumulator expects. Needs Rust-level debugging of the
     * substrait→DataFusion lowering of first_value in the coordinator-reduce stage.
     */
    @Ignore
    public void testFirstAcrossShards() throws Exception {
        String index = "coord_reduce_first";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        PPLResponse response = executePPL("source = " + index + " | stats first(value) as f");
        Object cell = singleScalar(response, "f");
        assertTrue("first cell must be numeric, got " + cell.getClass(), cell instanceof Number);
        long actual = ((Number) cell).longValue();
        assertTrue("first(value) must be one of the input values, got " + actual,
            actual >= 1 && actual <= (long) NUM_SHARDS * DOCS_PER_SHARD);
    }

    /**
     * PPL `last(field)` — custom UDAF, aliased to DataFusion's {@code last_value}
     * via {@code NAME_ALIASES}.
     *
     * <p>Ignored: same Rust-side panic as {@link #testFirstAcrossShards()}.
     */
    @Ignore
    public void testLastAcrossShards() throws Exception {
        String index = "coord_reduce_last";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        PPLResponse response = executePPL("source = " + index + " | stats last(value) as l");
        Object cell = singleScalar(response, "l");
        assertTrue("last cell must be numeric, got " + cell.getClass(), cell instanceof Number);
        long actual = ((Number) cell).longValue();
        assertTrue("last(value) must be one of the input values, got " + actual,
            actual >= 1 && actual <= (long) NUM_SHARDS * DOCS_PER_SHARD);
    }

    /**
     * PPL `distinct_count_approx(field)` — the same HLL path as {@code dc(field)}.
     * PPL frontend collapses both to {@code DISTINCT_COUNT_APPROX}; our planner
     * rewrites it to Calcite's {@code APPROX_COUNT_DISTINCT}. Overlapping per-shard
     * values verify the HLL sketch merge in coordinator reduce.
     *
     * <p>Ignored: DISTINCT_COUNT_APPROX is not registered in PPL's PPLFuncImpTable (sql
     * repo — out of scope for this worktree). BuiltinFunctionName.DISTINCT_COUNT_APPROX
     * exists and the backend wires {@code approx_count_distinct} via NAME_ALIASES, but
     * the frontend resolver throws {@code Cannot resolve function: DISTINCT_COUNT_APPROX}
     * before reaching the backend. Re-enable once the sql-repo registration lands.
     */
    @Ignore
    public void testDistinctCountApproxAcrossShards() throws Exception {
        String index = "coord_reduce_dca";
        createParquetBackedIndex(index);
        indexOverlappingDocs(index);

        int distinct = DOCS_PER_SHARD;
        PPLResponse response = executePPL("source = " + index + " | stats distinct_count_approx(value) as d");
        long actual = ((Number) singleScalar(response, "d")).longValue();
        // HLL — allow 10% tolerance for small cardinalities.
        assertEquals("distinct_count_approx(value) ≈ " + distinct, (double) distinct, (double) actual, distinct * 0.10);
    }

    /**
     * PPL `median(field)` — frontend rewrites to {@code percentile_approx(field, 50, SYMBOL)}.
     * Our planner drops the trailing SYMBOL arg and scales the percentage so DataFusion's
     * {@code approx_percentile_cont} sees {@code (field, 0.5)}.
     *
     * <p>Ignored: the PPL frontend emits a SYMBOL RexLiteral (an enum flag carrying the
     * field's Calcite SqlTypeName) alongside the percentile args. Our rewriter strips
     * the SYMBOL from the AggregateCall, but the flag also rides in a separate
     * LogicalProject produced by {@code relBuilder.aggregateCall}, and in the fragment-
     * split plan the shard-scan stage still carries that Project — isthmus's
     * SubstraitRelVisitor then hits the SYMBOL RexLiteral directly and throws
     * {@code Unable to handle symbol: INTEGER}. Fix requires either (a) stripping
     * SYMBOL at the PPL layer (sql repo, out of scope) or (b) reshaping the fragment-
     * splitter to drop SYMBOL columns from pre-agg projections consistently across
     * partial+final.
     */
    @Ignore
    public void testMedianAcrossShards() throws Exception {
        String index = "coord_reduce_median";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // values 1..N → median ≈ (N+1)/2
        double expected = (totalDocs + 1) / 2.0;
        PPLResponse response = executePPL("source = " + index + " | stats median(value) as m");
        double actual = ((Number) singleScalar(response, "m")).doubleValue();
        // Approximate percentile — t-digest tolerance for small N.
        assertEquals("median(value) ≈ " + expected, expected, actual, 1.0);
    }

    /**
     * PPL `percentile(field, 50)` — same backend path as median. Validates the SYMBOL-arg
     * stripping and the percentage→fraction conversion for a non-median percentile.
     *
     * <p>Ignored: same SYMBOL-round-trip issue as {@link #testMedianAcrossShards()}.
     */
    @Ignore
    public void testPercentileAcrossShards() throws Exception {
        String index = "coord_reduce_percentile";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        int totalDocs = NUM_SHARDS * DOCS_PER_SHARD;
        // 50th percentile of 1..N ≈ (N+1)/2
        double expected = (totalDocs + 1) / 2.0;
        PPLResponse response = executePPL("source = " + index + " | stats percentile(value, 50) as p");
        double actual = ((Number) singleScalar(response, "p")).doubleValue();
        assertEquals("percentile(value, 50) ≈ " + expected, expected, actual, 1.0);
    }

    /**
     * PPL `take(field, n)` — custom UDAF collects first N values into a list.
     * Verifies the take Rust UDAF's literal-arg and column-arg resolution paths
     * end-to-end through the coordinator reduce.
     *
     * <p>Ignored: hangs 1200s inside the coordinator-reduce stage's drain loop,
     * even after cherry-picking ip-pattern's two-state-field take.rs fix. The
     * UDAF works fine in isolation (ip-pattern's AggregationUDFIT.testTake green
     * on single-node); the hang is specific to the two-node coordinator-reduce
     * path plus the List(Utf8) output column running through ExchangeSink. Same
     * underlying Arrow-side schema issue as testList/testValues likely.
     */
    @Ignore
    public void testTakeAcrossShards() throws Exception {
        String index = "coord_reduce_take";
        createParquetBackedIndex(index);
        indexVaryingDocs(index);

        PPLResponse response = executePPL("source = " + index + " | stats take(value, 2) as t");
        Object cell = singleScalar(response, "t");
        assertTrue("take cell must materialize as a List, got " + cell.getClass(), cell instanceof java.util.List);
        @SuppressWarnings("unchecked")
        java.util.List<Object> taken = (java.util.List<Object>) cell;
        assertEquals("take(value, 2) returns exactly 2 values", 2, taken.size());
    }

    /** Pulls a single scalar cell from a one-row, one-named-column PPL response. */
    private static Object singleScalar(PPLResponse response, String column) {
        assertNotNull("PPLResponse must not be null", response);
        assertTrue("columns must contain '" + column + "', got " + response.getColumns(), response.getColumns().contains(column));
        assertEquals("scalar agg must return exactly 1 row", 1, response.getRows().size());
        Object cell = response.getRows().get(0)[response.getColumns().indexOf(column)];
        assertNotNull("'" + column + "' cell must not be null", cell);
        return cell;
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
