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
 * End-to-end IT for the whole-plan lowering plan format (whole-plan-lowering-spec.md).
 *
 * <p>With {@code analytics.engine.plan_format=whole_plan}, the coordinator converts the WHOLE
 * optimized distributed tree to one Substrait plan ({@code os_stage_boundary} markers at the
 * cuts), lowers it once via {@code planWholeQuery}, and cuts it into per-stage DataFusion
 * physical plans. The reduce / coordinator-local stage executes its cut plan via
 * {@code executeStageTask}; the shard stage ships legacy (shard proto execution is still gated),
 * so the reduce stage's {@code StageReadExec} schema — read off the whole-plan-lowered shard
 * subtree at the cut point — must line up with the legacy shard's actual Arrow output.
 *
 * <p>Crucially, because boundary schemas are correct by construction (read off the one lowered
 * tree, not reconciled from Calcite's declared rowType), the multi-shard PARTIAL→FINAL aggregate
 * that drifts Int32/Int64 under the legacy reduce sink should execute correctly here.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class WholePlanFormatIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "whole_plan_idx";

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
            // whole-plan-lowering-spec.md: lower the whole tree once and cut at boundaries.
            .put("analytics.engine.plan_format", "whole_plan")
            .build();
    }

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
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
     * Single-shard aggregate + filter under {@code whole_plan}: the whole tree is lowered once,
     * cut, and the coordinator-reduce stage executes its cut DataFusion plan. Result must equal
     * the legacy sum (600 over the three GET rows).
     */
    public void testAggregateUnderWholePlan() {
        createAndSeedHttpLogsIndex(1);
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql("SELECT 1 + 1 AS x, SUM(size) FROM " + INDEX + " WHERE verb = 'GET'");
        assertEquals("single aggregated row expected", 1, rows.size());
        Object[] row = rows.getFirst();
        assertEquals("x must be 1 + 1", 2, ((Number) row[0]).intValue());
        assertEquals("SUM(size) over verb='GET' rows must equal 600 under whole_plan", 600L, ((Number) row[1]).longValue());
    }

    /**
     * Multi-shard {@code SUM(size)} under {@code whole_plan}: a genuine distributed PARTIAL→FINAL
     * aggregate. Boundary schemas are correct by construction (the reduce stage's StageReadExec
     * schema is read off the whole-plan-lowered shard subtree, real Int64 — never Calcite's
     * declared Int32), so the result is correct without any boundary reconciliation.
     */
    public void testMultiShardSumUnderWholePlan() {
        createAndSeedHttpLogsIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql("SELECT SUM(size) FROM " + INDEX + " WHERE verb = 'GET'");
        assertEquals("single aggregated row expected", 1, rows.size());
        assertEquals(
            "multi-shard SUM(size) GET must equal 600 under whole_plan",
            600L,
            ((Number) rows.getFirst()[0]).longValue()
        );
    }
}
