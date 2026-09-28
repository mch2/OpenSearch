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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end integration test for the Calcite SQL front-end ({@code POST /_analytics/sql}).
 *
 * <p>Indexes a small dataset into a composite (parquet+lucene) index, then issues a representative
 * {@code SELECT ... WHERE ... GROUP BY ... ORDER BY ... LIMIT} through the SQL front-end and asserts
 * the returned {@code columns}/{@code rows}. This exercises the whole path: REST → transport → Calcite
 * parse/validate/convert against AE's schema → AE plan execution → response.
 */
public class AnalyticsSqlEndToEndIT extends AnalyticsRestTestCase {

    private static final String INDEX = "sql_e2e";
    private static final int SHARDS = 2;
    private static boolean provisioned = false;

    private void ensureData() throws IOException {
        if (provisioned) return;
        createCompositeIndex(INDEX, SHARDS, "{\"dept\": {\"type\": \"keyword\"}, \"salary\": {\"type\": \"integer\"}}");
        StringBuilder bulk = new StringBuilder();
        String[] depts = { "eng", "eng", "sales", "sales", "sales", "hr" };
        int[] salaries = { 100, 120, 90, 80, 85, 70 };
        for (int i = 0; i < depts.length; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"dept\":\"").append(depts[i]).append("\",\"salary\":").append(salaries[i]).append("}\n");
        }
        bulkAndRefresh(INDEX, bulk.toString());
        provisioned = true;
    }

    public void testSelectWhereGroupByOrderByLimit() throws IOException {
        ensureData();
        // Average salary per department for departments with salary > 75, ordered by department,
        // limited to the top 2.
        String sql = "SELECT dept, COUNT(*) AS c FROM "
            + INDEX
            + " WHERE salary > 75 GROUP BY dept ORDER BY dept LIMIT 2";
        SqlResult result = runAnalyticsSql(sql);
        assertTrue("columns must include dept", result.columns().contains("dept"));
        assertTrue("columns must include c", result.columns().contains("c"));
        // eng (1 row >75: 100,120 → 2) and hr (70 excluded) → eng, sales are the >75 groups; ordered
        // ascending, limit 2 → eng, sales.
        assertEquals("two groups returned", 2, result.rows().size());
        int deptCol = result.columns().indexOf("dept");
        assertEquals("first group is eng", "eng", String.valueOf(result.rows().get(0).get(deptCol)));
        assertEquals("second group is sales", "sales", String.valueOf(result.rows().get(1).get(deptCol)));
    }

    public void testSimpleProjection() throws IOException {
        ensureData();
        SqlResult result = runAnalyticsSql("SELECT dept, salary FROM " + INDEX + " WHERE dept = 'hr'");
        assertEquals(List.of("dept", "salary"), result.columns());
        assertEquals("one hr row", 1, result.rows().size());
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

    private record SqlResult(List<String> columns, List<List<Object>> rows) {}

    @SuppressWarnings("unchecked")
    private SqlResult runAnalyticsSql(String sql) throws IOException {
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson(sql) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "SQL: " + sql);
        List<String> columns = (List<String>) body.get("columns");
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("response missing 'columns' for: " + sql, columns);
        assertNotNull("response missing 'rows' for: " + sql, rows);
        return new SqlResult(columns, rows);
    }

    private void createCompositeIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": " + mappingProperties + " }"
            + "}";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index " + name);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));
        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void bulkAndRefresh(String indexName, String bulkBody) throws IOException {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }
}
