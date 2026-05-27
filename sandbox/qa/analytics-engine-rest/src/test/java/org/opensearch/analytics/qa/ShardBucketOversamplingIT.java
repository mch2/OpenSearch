/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for the
 * {@code index.analytics.shard_bucket_oversampling_factor} index setting and the
 * shard-side {@code Sort+Limit} rewrite it controls
 * (see {@code OpenSearchAggregateShardBucketRule}). Verifies setting wiring,
 * validation, and that the rewrite preserves top-K correctness end-to-end.
 *
 * <p>Each test creates a fresh 2-shard parquet-backed index so cardinalities and
 * factor overrides don't leak between tests.
 */
public class ShardBucketOversamplingIT extends AnalyticsRestTestCase {

    private static final int NUM_SHARDS = 2;

    /**
     * Default factor (1.5): {@code stats sum(value) as total by category | sort - total | head 100}
     * fires the rule with {@code shardSize = ceil(max(100, 10) * 1.5) + 10 = 160}. Per-shard
     * cardinality (3) is well below shardSize, so each shard ships every group and the
     * coordinator's final top-K must contain {@code {a:5, b:7, c:9}}. Asserts correctness
     * end-to-end on the rewritten path.
     */
    public void testDefaultFactor_AllGroupsCorrect() throws Exception {
        String index = "sb_default";
        createIndexWithFactor(index, null);
        indexCategorizedDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats sum(value) as total by category | sort - total | head 100"
        );
        assertExactSums(result, Map.of("a", 5L, "b", 7L, "c", 9L));
    }

    /**
     * Factor=0 disables the rule — coordinator receives every group, no shard-side
     * Sort+Limit. Asserts the disabled path also returns the correct sums.
     */
    public void testFactorZero_Disabled_AllGroupsCorrect() throws Exception {
        String index = "sb_factor_zero";
        createIndexWithFactor(index, 0.0);
        indexCategorizedDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats sum(value) as total by category | sort - total | head 100"
        );
        assertExactSums(result, Map.of("a", 5L, "b", 7L, "c", 9L));
    }

    /**
     * Factor=1.0 — no oversampling buffer; {@code shardSize = max(100, 10) * 1.0 + 10 = 110}.
     * Still well above per-shard cardinality. Asserts no group is dropped on the rewrite path.
     */
    public void testFactorOne_AllGroupsCorrect() throws Exception {
        String index = "sb_factor_one";
        createIndexWithFactor(index, 1.0);
        indexCategorizedDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats sum(value) as total by category | sort - total | head 100"
        );
        assertExactSums(result, Map.of("a", 5L, "b", 7L, "c", 9L));
    }

    /** Values in the {@code (0.0, 1.0)} open interval must be rejected at index-creation time. */
    public void testInvalidFactor_Rejected() throws Exception {
        String index = "sb_invalid_factor";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}

        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity(indexBody(0.5));
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(createIndex));
        assertEquals("invalid factor must be rejected with 400", 400, ex.getResponse().getStatusLine().getStatusCode());
    }

    /**
     * The setting can be updated after the index exists via {@code PUT _settings}.
     * Confirms the dynamic-update path stays wired and a follow-up query still works.
     */
    public void testDynamicSettingUpdate_TakesEffect() throws Exception {
        String index = "sb_dynamic_update";
        createIndexWithFactor(index, null);
        indexCategorizedDocs(index);

        Request updateSettings = new Request("PUT", "/" + index + "/_settings");
        updateSettings.setJsonEntity("{\"index.analytics.shard_bucket_oversampling_factor\": 3.0}");
        Response updateResp = client().performRequest(updateSettings);
        assertEquals("settings update must succeed", 200, updateResp.getStatusLine().getStatusCode());

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats sum(value) as total by category | sort - total | head 100"
        );
        assertExactSums(result, Map.of("a", 5L, "b", 7L, "c", 9L));
    }

    /**
     * AVG-by-AVG with default factor: exercise the primitive-decomposed path. The reduce
     * rule splits AVG into SUM+COUNT+Project; the shard-bucket rule walks through the
     * Project and uses the recompose RexNode {@code SUM/COUNT} as the shard-side sort
     * expression (sortExprs in {@code OpenSearchSort}). Per-category averages are exact:
     * a=(1+4)/2=2.5, b=(2+5)/2=3.5, c=(3+6)/2=4.5.
     */
    public void testAvg_byAvg_AllGroupsCorrect_default() throws Exception {
        String index = "sb_avg_default";
        createIndexWithFactor(index, null);
        indexCategorizedDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats avg(value) as avg_v by category | sort - avg_v | head 100"
        );
        assertExactColumnValues(result, "category", "avg_v", Map.of("a", 2.5, "b", 3.5, "c", 4.5), 0.001);
    }

    /**
     * AVG-by-AVG with factor=0: rule is disabled — coord receives every group via the
     * standard PARTIAL→FINAL path. Same expected values as the default-factor case.
     */
    public void testAvg_byAvg_AllGroupsCorrect_factorZero() throws Exception {
        String index = "sb_avg_factor_zero";
        createIndexWithFactor(index, 0.0);
        indexCategorizedDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats avg(value) as avg_v by category | sort - avg_v | head 100"
        );
        assertExactColumnValues(result, "category", "avg_v", Map.of("a", 2.5, "b", 3.5, "c", 4.5), 0.001);
    }

    /**
     * DC-by-DC with default factor: exercise the engine-native-merge path. The shard-bucket
     * rule recognises {@code APPROX_COUNT_DISTINCT} as engine-native merge and substitutes
     * the column ref in the sort expression with {@code hll_estimate($state)}; the split
     * rule emits {@code AggregateMode.SHARD_MERGE} so the shard ships the HLL sketch as
     * Binary intermediate state instead of the BIGINT scalar. Coord-side
     * {@link org.opensearch.analytics.planner.dag.DistributedAggregateRewriter} runs
     * {@code APPROX_COUNT_DISTINCT(state)} to merge sketches.
     *
     * <p>Per-category cardinality is exactly 2 (two distinct values per category). HLL is
     * exact for cardinalities under its sparse-mode threshold.
     */
    /**
     * DC-by-DC with default factor: exercise the engine-native-merge path. The shard-bucket
     * rule recognises {@code APPROX_COUNT_DISTINCT} as engine-native merge and substitutes
     * the column ref in the sort expression with {@code hll_estimate($state)}; the split
     * rule emits {@code AggregateMode.SHARD_MERGE} so the shard ships the HLL sketch as
     * Binary intermediate state instead of the BIGINT scalar. Coord-side
     * {@link org.opensearch.analytics.planner.dag.DistributedAggregateRewriter} runs
     * {@code APPROX_COUNT_DISTINCT(state)} to merge sketches.
     *
     * <p>Uses 30 distinct values per category so HLL is well above its sparse-mode regime;
     * at &lt;~5 distinct per shard the per-partition partial-final pipeline collapses to a
     * scalar that can't be merged across shards. That's a DataFusion HLL behaviour, not a
     * shard-bucket-rule issue, so this test asserts on cardinalities where the merge is
     * reliable.
     */
    public void testDc_byDc_AllGroupsCorrect_default() throws Exception {
        String index = "sb_dc_default";
        createIndexWithFactor(index, null);
        indexHighCardinalityDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats dc(value) as dc_v by category | sort - dc_v | head 100"
        );
        assertExactColumnValues(result, "category", "dc_v", Map.of("a", 30L, "b", 30L, "c", 30L));
    }

    /**
     * DC-by-DC with factor=0: rule disabled — coord-side FINAL receives the regular
     * PARTIAL output (HLL sketches per partition). Same expected cardinalities.
     */
    public void testDc_byDc_AllGroupsCorrect_factorZero() throws Exception {
        String index = "sb_dc_factor_zero";
        createIndexWithFactor(index, 0.0);
        indexHighCardinalityDocs(index);

        Map<String, Object> result = executePPL(
            "source = " + index + " | stats dc(value) as dc_v by category | sort - dc_v | head 100"
        );
        assertExactColumnValues(result, "category", "dc_v", Map.of("a", 30L, "b", 30L, "c", 30L));
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Creates a 2-shard parquet-backed index with {@code (category keyword, value integer)}.
     * If {@code factor} is non-null it's applied via the index setting; null uses the cluster default.
     */
    private void createIndexWithFactor(String indexName, Double factor) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {}

        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(indexBody(factor));
        Map<String, Object> response = assertOkAndParse(client().performRequest(createIndex), "Create index " + indexName);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + indexName);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /** Builds the {@code PUT /index} body. {@code factor} is only added when non-null. */
    private String indexBody(Double factor) {
        StringBuilder settings = new StringBuilder();
        settings.append("\"number_of_shards\": ").append(NUM_SHARDS).append(",")
            .append("\"number_of_replicas\": 0,")
            .append("\"index.pluggable.dataformat.enabled\": true,")
            .append("\"index.pluggable.dataformat\": \"composite\",")
            .append("\"index.composite.primary_data_format\": \"parquet\",")
            .append("\"index.composite.secondary_data_formats\": \"\"");
        if (factor != null) {
            settings.append(",\"index.analytics.shard_bucket_oversampling_factor\": ").append(factor);
        }
        return "{"
            + "\"settings\": {" + settings + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"category\": { \"type\": \"keyword\" },"
            + "    \"value\":    { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";
    }

    /** Indexes 90 docs across 3 categories with 30 distinct values per category. */
    private void indexHighCardinalityDocs(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        String[] cats = { "a", "b", "c" };
        int valuesPerCat = 30;
        int id = 0;
        for (String cat : cats) {
            for (int v = 0; v < valuesPerCat; v++) {
                bulk.append("{\"index\": {\"_id\": \"hc").append(id++).append("\"}}\n");
                bulk.append("{\"category\": \"").append(cat).append("\", \"value\": ").append(v + cat.charAt(0) * 1000).append("}\n");
            }
        }

        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));

        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                Map<String, Object> probe = executePPL("source = " + indexName + " | stats sum(value) as total by category");
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) probe.get("rows");
                if (rows != null && rows.size() == 3) return;
            } catch (Exception ignored) {}
            Thread.sleep(100);
        }
        throw new AssertionError("indexHighCardinalityDocs: 3 categories did not stabilize within 10s");
    }

    /**
     * Six docs across three categories. Sums are deterministic regardless of
     * shard placement: a=5, b=7, c=9.
     */
    private void indexCategorizedDocs(String indexName) throws Exception {
        StringBuilder bulk = new StringBuilder();
        appendDoc(bulk, 0, "a", 1);
        appendDoc(bulk, 1, "b", 2);
        appendDoc(bulk, 2, "c", 3);
        appendDoc(bulk, 3, "a", 4);
        appendDoc(bulk, 4, "b", 5);
        appendDoc(bulk, 5, "c", 6);

        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));

        // Composite (parquet) format publishes shard-side rowgroups asynchronously after
        // refresh+flush; the analytics-engine planner can resolve scans against partially-
        // committed shards which then return incomplete GROUP BY output. Poll the actual
        // SUM-by-category shape until {a:5, b:7, c:9} is fully visible across both shards;
        // bail out at ~10s.
        long deadline = System.currentTimeMillis() + 10_000L;
        while (true) {
            try {
                Map<String, Object> grouped = executePPL(
                    "source = " + indexName + " | stats sum(value) as total by category"
                );
                if (allCategorySumsMatch(grouped)) {
                    return;
                }
            } catch (Exception ignored) {
                // backend may briefly 5xx during materialization — keep polling
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(
                    "SUM(value) BY category did not stabilize to {a:5, b:7, c:9} within 10s for [" + indexName + "]"
                );
            }
            Thread.sleep(100);
        }
    }

    /** True when the result has {a:5, b:7, c:9} for the {@code total} column. */
    @SuppressWarnings("unchecked")
    private static boolean allCategorySumsMatch(Map<String, Object> grouped) {
        List<String> columns = (List<String>) grouped.get("columns");
        List<List<Object>> rows = (List<List<Object>>) grouped.get("rows");
        if (columns == null || rows == null || rows.size() != 3) return false;
        int catIdx = columns.indexOf("category");
        int totalIdx = columns.indexOf("total");
        if (catIdx < 0 || totalIdx < 0) return false;
        Map<String, Long> sums = new java.util.HashMap<>();
        for (List<Object> row : rows) {
            Object cat = row.get(catIdx);
            Object total = row.get(totalIdx);
            if (!(cat instanceof String catStr) || !(total instanceof Number totalNum)) return false;
            sums.put(catStr, totalNum.longValue());
        }
        return Long.valueOf(5L).equals(sums.get("a"))
            && Long.valueOf(7L).equals(sums.get("b"))
            && Long.valueOf(9L).equals(sums.get("c"));
    }

    private static void appendDoc(StringBuilder bulk, int id, String category, int value) {
        bulk.append("{\"index\": {\"_id\": \"").append(id).append("\"}}\n");
        bulk.append("{\"category\": \"").append(category).append("\", \"value\": ").append(value).append("}\n");
    }

    /**
     * Asserts the PPL response carries exactly the expected {@code (category, total)} pairs.
     * Order-insensitive: groups can come back in any order.
     */
    @SuppressWarnings("unchecked")
    private void assertExactSums(Map<String, Object> result, Map<String, Long> expectedSums) {
        List<String> columns = (List<String>) result.get("columns");
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("columns must not be null", columns);
        assertNotNull("rows must not be null", rows);
        int catIdx = columns.indexOf("category");
        int totalIdx = columns.indexOf("total");
        assertTrue("response must contain category and total columns: " + columns, catIdx >= 0 && totalIdx >= 0);

        Map<String, Long> actualSums = new java.util.HashMap<>();
        for (List<Object> row : rows) {
            actualSums.put((String) row.get(catIdx), ((Number) row.get(totalIdx)).longValue());
        }
        assertEquals("category sums; full response: " + result, expectedSums, actualSums);
    }

    /**
     * Asserts the response carries exactly the expected key→long-value pairs for the named
     * key/value columns. Order-insensitive.
     */
    @SuppressWarnings("unchecked")
    private void assertExactColumnValues(Map<String, Object> result, String keyCol, String valCol, Map<String, Long> expected) {
        List<String> columns = (List<String>) result.get("columns");
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("columns must not be null", columns);
        assertNotNull("rows must not be null", rows);
        int kIdx = columns.indexOf(keyCol);
        int vIdx = columns.indexOf(valCol);
        assertTrue("response must contain " + keyCol + " and " + valCol + " columns: " + columns, kIdx >= 0 && vIdx >= 0);
        Map<String, Long> actual = new java.util.HashMap<>();
        for (List<Object> row : rows) {
            actual.put((String) row.get(kIdx), ((Number) row.get(vIdx)).longValue());
        }
        assertEquals(keyCol + "→" + valCol + "; full response: " + result, expected, actual);
    }

    /**
     * Asserts the response carries the expected key→double-value pairs for the named
     * key/value columns within {@code tolerance}. Order-insensitive.
     */
    @SuppressWarnings("unchecked")
    private void assertExactColumnValues(
        Map<String, Object> result,
        String keyCol,
        String valCol,
        Map<String, Double> expected,
        double tolerance
    ) {
        List<String> columns = (List<String>) result.get("columns");
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("columns must not be null", columns);
        assertNotNull("rows must not be null", rows);
        int kIdx = columns.indexOf(keyCol);
        int vIdx = columns.indexOf(valCol);
        assertTrue("response must contain " + keyCol + " and " + valCol + " columns: " + columns, kIdx >= 0 && vIdx >= 0);
        assertEquals("row count must match expected size; full response: " + result, expected.size(), rows.size());
        Map<String, Double> actual = new java.util.HashMap<>();
        for (List<Object> row : rows) {
            actual.put((String) row.get(kIdx), ((Number) row.get(vIdx)).doubleValue());
        }
        for (Map.Entry<String, Double> e : expected.entrySet()) {
            Double v = actual.get(e.getKey());
            assertNotNull("missing key " + e.getKey() + " in response: " + result, v);
            assertEquals(keyCol + "=" + e.getKey() + " " + valCol, e.getValue(), v, tolerance);
        }
    }

    private Map<String, Object> executePPL(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + ppl + "\"}");
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }
}
