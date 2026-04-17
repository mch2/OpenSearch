/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * End-to-end integration test covering the single-shard fragment dispatch
 * path through the streaming transport.
 *
 * <p>Pipeline: PPL → planner → DAG builder → FragmentConversion →
 * ShardFragmentStageExecution → streaming transport → AnalyticsSearchService →
 * DataFusion native engine → stream of Arrow batches → client-side zero-copy
 * transfer into sink → PPL response.
 *
 * <p>Single shard + no coordinator reduce, so the CBO produces a root-only
 * DAG. Each shard serves the bundled {@code clickbench_hits_100.parquet}
 * fixture (100 rows with real ClickBench schema) via
 * {@link org.opensearch.be.datafusion.DatafusionReaderManager}'s indexing-mock
 * fallback, triggered by a refresh after indexing a dummy doc.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class AnalyticsShardDispatchIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AnalyticsShardDispatchIT.class);
    private static final String TEST_INDEX = "parquet_simple";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                AnalyticsPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                AnalyticsPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            new PluginInfo(
                ParquetDataFormatPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                ParquetDataFormatPlugin.class.getName(),
                null,
                Collections.emptyList(),
                false
            ),
            new PluginInfo(
                DataFusionPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                DataFusionPlugin.class.getName(),
                null,
                List.of(AnalyticsPlugin.class.getName(), ParquetDataFormatPlugin.class.getName()),
                false
            )
        );
    }

    private void createTestIndex() {
        if (indexExists(TEST_INDEX)) return;
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        )
            .setMapping(
                "Age",
                "type=short",
                "AdvEngineID",
                "type=short",
                "RegionID",
                "type=integer",
                "Title",
                "type=keyword",
                "URL",
                "type=keyword"
            )
            .get();
        ensureGreen(TEST_INDEX);

        // Index a dummy doc and refresh so DatafusionReaderManager's afterRefresh(didRefresh=true)
        // fallback registers the bundled clickbench_hits_100.parquet as the shard's data source.
        client().prepareIndex(TEST_INDEX).setSource("Age", 0).get();
        client().admin().indices().prepareRefresh(TEST_INDEX).get();
    }

    /**
     * Fields projection returns all 100 rows from the mock parquet.
     * Exercises: streaming dispatch, VSR transfer, sink accumulation.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testFieldsProjection() throws Exception {
        createTestIndex();

        PPLRequest req = new PPLRequest("source = " + TEST_INDEX + " | fields Title, URL");
        PPLResponse resp = client().execute(UnifiedPPLExecuteAction.INSTANCE, req).actionGet();

        assertNotNull("response", resp);
        assertNotNull("columns", resp.getColumns());
        assertEquals("column count", 2, resp.getColumns().size());
        assertTrue("contains Title", resp.getColumns().contains("Title"));
        assertTrue("contains URL", resp.getColumns().contains("URL"));
        assertEquals("row count", 100, resp.getRows().size());
    }

    /**
     * COUNT aggregate over the mock parquet returns 100.
     * Exercises: aggregate execution on a single shard, single-row response.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testCountAggregate() throws Exception {
        createTestIndex();

        PPLRequest req = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt");
        PPLResponse resp = client().execute(UnifiedPPLExecuteAction.INSTANCE, req).actionGet();

        assertNotNull("response", resp);
        assertTrue("columns contain cnt", resp.getColumns().contains("cnt"));
        assertEquals("row count", 1, resp.getRows().size());

        int idx = resp.getColumns().indexOf("cnt");
        long cnt = ((Number) resp.getRows().get(0)[idx]).longValue();
        assertEquals("COUNT", 100, cnt);
    }

    /**
     * SUM(Age) over the mock parquet returns 4275.
     * Exercises: aggregate with a numeric value in the response — catches
     * transfer/validity-buffer issues that previously silently returned null
     * for the aggregate cell.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSumAggregate() throws Exception {
        createTestIndex();

        PPLRequest req = new PPLRequest("source = " + TEST_INDEX + " | stats sum(Age) as total_age");
        PPLResponse resp = client().execute(UnifiedPPLExecuteAction.INSTANCE, req).actionGet();

        assertNotNull("response", resp);
        assertTrue("columns contain total_age", resp.getColumns().contains("total_age"));
        assertEquals("row count", 1, resp.getRows().size());

        int idx = resp.getColumns().indexOf("total_age");
        Object cell = resp.getRows().get(0)[idx];
        assertNotNull("total_age should not be null", cell);
        long totalAge = ((Number) cell).longValue();
        assertEquals("SUM(Age) for clickbench_hits_100.parquet", 4275L, totalAge);
    }
}
