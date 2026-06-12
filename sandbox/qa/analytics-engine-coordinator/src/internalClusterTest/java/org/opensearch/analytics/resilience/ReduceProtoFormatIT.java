/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end IT for the df-proto migration's {@code reduce_proto} plan format (D12).
 *
 * <p>With {@code analytics.engine.plan_format=reduce_proto}, the coordinator additionally
 * finalizes reduce / coordinator-local stages to serialized DataFusion physical plans —
 * driving the full native finalizer (lower → mode-force → §4.1 graft → leaf-rewrite →
 * codec encode → debug round-trip assert) over real corpus plans on a live cluster. The
 * finalized plan is attached; execution still flows through the existing path, so results
 * MUST equal the legacy path — finalizing to proto is transparent.
 *
 * <p>Single-shard on purpose: the multi-shard PARTIAL→FINAL reduce sink has a known
 * pre-existing Int32/Int64 width drift (documented in {@code ValuesSqlIT}) being fixed
 * separately; this IT validates that proto finalization itself is correct and side-effect
 * free, not that unrelated reduce-sink bug.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class ReduceProtoFormatIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "reduce_proto_idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
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
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            // df-proto migration D12: finalize reduce/coordinator stages to proto.
            .put("analytics.engine.plan_format", "reduce_proto")
            .build();
    }

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createAndSeedHttpLogsIndex() {
        createAndSeedHttpLogsIndex(1);
    }

    private void createAndSeedHttpLogsIndex(int shards) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
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
            .setMapping("verb", "type=keyword,index=false", "size", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        Object[][] docs = { { "GET", 100 }, { "POST", 50 }, { "GET", 200 }, { "GET", 300 } };
        for (int i = 0; i < docs.length; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("verb", docs[i][0], "size", docs[i][1]).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    /**
     * Aggregate + filter under {@code reduce_proto}: the coordinator-local aggregate stage
     * is finalized to a DataFusion proto plan (exercising the full native finalizer over a
     * real corpus plan), then executed. Result must equal the legacy sum.
     */
    public void testAggregateUnderReduceProto() {
        createAndSeedHttpLogsIndex();
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql("SELECT 1 + 1 AS x, SUM(size) FROM " + INDEX + " WHERE verb = 'GET'");
        assertEquals("single aggregated row expected", 1, rows.size());
        Object[] row = rows.getFirst();
        assertEquals("x must be 1 + 1", 2, ((Number) row[0]).intValue());
        assertEquals("SUM(size) over verb='GET' rows must equal 600 under reduce_proto", 600L, ((Number) row[1]).longValue());
    }

    /**
     * Multi-shard {@code SUM(size)} under {@code reduce_proto}: a genuine distributed
     * PARTIAL→FINAL aggregate. The shard stage stays legacy and emits Int64 partial state
     * while Calcite declares the boundary as Int32. The finalizer resolves this via D5: it
     * derives the child's ACTUAL partial-output schema coordinator-side (from the child's
     * partial Substrait, supplied in StageMeta) and stamps the reduce stage's StageReadExec —
     * and plans the FINAL aggregate — over the real Int64 type. No Substrait reconciliation at
     * the boundary, so the pre-existing Int32/Int64 reduce-sink drift is avoided.
     */
    public void testMultiShardSumUnderReduceProto() {
        createAndSeedHttpLogsIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql("SELECT SUM(size) FROM " + INDEX + " WHERE verb = 'GET'");
        assertEquals("single aggregated row expected", 1, rows.size());
        assertEquals(
            "multi-shard SUM(size) GET must equal 600 under reduce_proto",
            600L,
            ((Number) rows.getFirst()[0]).longValue()
        );
    }
}
