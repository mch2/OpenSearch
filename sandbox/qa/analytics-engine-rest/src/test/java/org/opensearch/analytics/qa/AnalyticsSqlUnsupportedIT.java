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

import java.io.IOException;
import java.util.Map;

/**
 * Verifies the SQL front-end rejects non-query statements (DDL/DML) with a 400-class error (R5):
 * v1 supports read queries only.
 */
public class AnalyticsSqlUnsupportedIT extends AnalyticsRestTestCase {

    private static final String INDEX = "sql_unsupported";
    private static boolean provisioned = false;

    private void ensureData() throws IOException {
        if (provisioned) return;
        createCompositeIndex(INDEX, 1, "{\"a\": {\"type\": \"integer\"}}");
        bulkAndRefresh(INDEX, "{\"index\":{}}\n{\"a\":1}\n");
        provisioned = true;
    }

    public void testInsertRejected() throws IOException {
        ensureData();
        assertRejected("INSERT INTO " + INDEX + " (a) VALUES (2)");
    }

    public void testCreateRejected() throws IOException {
        ensureData();
        assertRejected("CREATE TABLE " + INDEX + "_new (a INT)");
    }

    private void assertRejected(String sql) throws IOException {
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson(sql) + "\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(request));
        Response response = ex.getResponse();
        int status = response.getStatusLine().getStatusCode();
        assertEquals("unsupported statement must be a 400-class client error for: " + sql, 400, status);
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

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
