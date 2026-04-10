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

/**
 * End-to-end integration test for the analytics shard dispatch pipeline
 * using the DataFusion backend with mock parquet data.
 *
 * <p>Exercises the full pipeline: PPL parsing → Calcite planning → backend marking →
 * DAG construction → Substrait fragment conversion → shard dispatch → DataFusion
 * native execution → Arrow result collection → PPL response.
 *
 * <p>DataFusion's mock parquet reader serves 100 rows of pre-generated data
 * (id, name, age, score, city) regardless of index contents. The index only
 * needs to exist for schema registration.
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
                DataFusionPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                DataFusionPlugin.class.getName(),
                null,
                List.of(AnalyticsPlugin.class.getName()),
                false
            )
        );
    }

    /**
     * Creates the test index with the schema matching DataFusion's mock parquet data.
     * No data ingestion needed — DataFusion's mock reader serves 100 pre-generated rows.
     */
    private void createTestIndex() {
        prepareCreate(TEST_INDEX).setSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).setMapping("id", "type=long", "name", "type=keyword", "age", "type=long", "score", "type=long", "city", "type=keyword").get();
        ensureGreen(TEST_INDEX);
    }

    /**
     * Scan + project: fields name, city → 100 rows, 2 columns from DataFusion mock data.
     */
    public void testFieldsProjection() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | fields name, city");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertEquals("Should have 2 columns", 2, response.getColumns().size());
        assertTrue("Columns should contain 'name'", response.getColumns().contains("name"));
        assertTrue("Columns should contain 'city'", response.getColumns().contains("city"));
        assertEquals("Should have 100 rows from mock parquet data", 100, response.getRows().size());
    }

    /**
     * COUNT(*) → 100 (DataFusion's mock parquet has 100 rows).
     */
    public void testCountAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'cnt'", response.getColumns().contains("cnt"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int cntIdx = response.getColumns().indexOf("cnt");
        long cnt = ((Number) response.getRows().get(0)[cntIdx]).longValue();
        assertEquals("COUNT should be 100", 100, cnt);
    }

    /**
     * SUM(age) → 4228 (known sum from DataFusion's mock parquet data).
     */
    public void testSumAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats sum(age) as total_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'total_age'", response.getColumns().contains("total_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int idx = response.getColumns().indexOf("total_age");
        long totalAge = ((Number) response.getRows().get(0)[idx]).longValue();
        assertEquals("SUM(age) should be 4228", 4228, totalAge);
    }

    /**
     * MIN/MAX age → min=18, max=65 from DataFusion's mock parquet data.
     */
    public void testMinMaxAggregate() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats min(age) as min_age, max(age) as max_age");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'min_age'", response.getColumns().contains("min_age"));
        assertTrue("Columns should contain 'max_age'", response.getColumns().contains("max_age"));
        assertEquals("Should have 1 row", 1, response.getRows().size());

        int minIdx = response.getColumns().indexOf("min_age");
        int maxIdx = response.getColumns().indexOf("max_age");
        assertEquals("MIN(age) should be 18", 18, ((Number) response.getRows().get(0)[minIdx]).longValue());
        assertEquals("MAX(age) should be 65", 65, ((Number) response.getRows().get(0)[maxIdx]).longValue());
    }

    /**
     * COUNT(*) GROUP BY city → 5 cities with known counts from mock parquet data.
     */
    public void testCountGroupBy() throws Exception {
        createTestIndex();

        PPLRequest pplRequest = new PPLRequest("source = " + TEST_INDEX + " | stats count() as cnt by city");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest).actionGet();

        assertNotNull("PPLResponse should not be null", response);
        assertTrue("Columns should contain 'cnt'", response.getColumns().contains("cnt"));
        assertTrue("Columns should contain 'city'", response.getColumns().contains("city"));
        assertEquals("Should have 5 city groups", 5, response.getRows().size());

        int cntIdx = response.getColumns().indexOf("cnt");
        int cityIdx = response.getColumns().indexOf("city");

        // Collect results into a map for flexible assertion
        java.util.Map<String, Long> cityCounts = new java.util.HashMap<>();
        for (Object[] row : response.getRows()) {
            String city = String.valueOf(row[cityIdx]);
            long cnt = ((Number) row[cntIdx]).longValue();
            cityCounts.put(city, cnt);
        }

        assertEquals("paris count", 12, (long) cityCounts.get("paris"));
        assertEquals("tokyo count", 22, (long) cityCounts.get("tokyo"));
        assertEquals("berlin count", 26, (long) cityCounts.get("berlin"));
        assertEquals("new york count", 22, (long) cityCounts.get("new york"));
        assertEquals("london count", 18, (long) cityCounts.get("london"));
    }
}
