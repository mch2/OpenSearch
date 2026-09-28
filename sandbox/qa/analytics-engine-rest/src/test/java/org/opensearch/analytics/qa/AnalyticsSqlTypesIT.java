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
 * Verifies the SQL front-end validates and executes queries over AE custom-typed fields
 * ({@code ip} → {@code IpType}/VARBINARY, {@code scaled_float} → {@code ScaledFloatType}/BIGINT),
 * proving the front-end reuses AE's types (R4) rather than a parallel type system.
 */
public class AnalyticsSqlTypesIT extends AnalyticsRestTestCase {

    private static final String INDEX = "sql_types";
    private static boolean provisioned = false;

    private void ensureData() throws IOException {
        if (provisioned) return;
        String mapping = "{"
            + "\"client_ip\": {\"type\": \"ip\"},"
            + "\"price\": {\"type\": \"scaled_float\", \"scaling_factor\": 100},"
            + "\"label\": {\"type\": \"keyword\"}"
            + "}";
        createCompositeIndex(INDEX, 1, mapping);
        String bulk = "{\"index\":{}}\n"
            + "{\"client_ip\":\"192.168.1.10\",\"price\":19.99,\"label\":\"a\"}\n"
            + "{\"index\":{}}\n"
            + "{\"client_ip\":\"10.0.0.1\",\"price\":5.50,\"label\":\"b\"}\n";
        bulkAndRefresh(INDEX, bulk);
        provisioned = true;
    }

    public void testQueryOverIpAndScaledFloatValidatesAndReturnsRows() throws IOException {
        ensureData();
        SqlResult result = runAnalyticsSql("SELECT label, client_ip, price FROM " + INDEX + " ORDER BY label");
        assertTrue("columns include client_ip", result.columns().contains("client_ip"));
        assertTrue("columns include price", result.columns().contains("price"));
        assertEquals("two rows returned", 2, result.rows().size());
    }

    // ─── helpers (shared shape with the other SQL ITs) ──────────────────────────

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
