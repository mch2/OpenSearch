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
import java.util.List;
import java.util.Map;

/**
 * Fail-closed contract at the HTTP boundary (R2, R4): a standard SQL function the analytics engine
 * cannot execute is rejected with a 400 that NAMES the function (rather than validating and then
 * failing opaquely deep in execution), while an AE-supported function returns correct results —
 * proving the guard rejects the right side of the boundary and only that side.
 */
public class AnalyticsSqlUnsupportedFunctionIT extends AnalyticsRestTestCase {

    private static final String INDEX = "sql_failclosed";
    private static boolean provisioned = false;

    private void ensureData() throws IOException {
        if (provisioned) return;
        createCompositeIndex(INDEX, 1, "{\"dept\": {\"type\": \"keyword\"}, \"salary\": {\"type\": \"integer\"}}");
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\":{}}\n{\"dept\":\"eng\",\"salary\":100}\n");
        bulk.append("{\"index\":{}}\n{\"dept\":\"eng\",\"salary\":120}\n");
        bulk.append("{\"index\":{}}\n{\"dept\":\"hr\",\"salary\":70}\n");
        bulkAndRefresh(INDEX, bulk.toString());
        provisioned = true;
    }

    /** BIT_XOR is a standard SQL aggregate AE does not implement — rejected with 400, named in the body. */
    public void testUnsupportedStandardAggregateRejectedNamingIt() throws IOException {
        ensureData();
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson("SELECT BIT_XOR(salary) FROM " + INDEX) + "\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(request));
        Response response = ex.getResponse();
        assertEquals(400, response.getStatusLine().getStatusCode());
        String body = org.opensearch.common.io.Streams.readFully(response.getEntity().getContent()).utf8ToString();
        assertTrue("400 body should name the unsupported function; got: " + body, body.contains("BIT_XOR"));
    }

    /** The bitwise-XOR operator is a standard operator AE does not implement — rejected with 400. */
    public void testUnsupportedStandardScalarRejected() throws IOException {
        ensureData();
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson("SELECT salary ^ 1 FROM " + INDEX) + "\"}");
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
    }

    /** An AE-supported aggregate (COUNT) over the same index returns correct results — the other side of the boundary. */
    public void testSupportedFunctionReturnsResults() throws IOException {
        ensureData();
        Request request = new Request("POST", "/_analytics/sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson("SELECT dept, COUNT(*) c FROM " + INDEX + " GROUP BY dept ORDER BY dept") + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "supported COUNT query");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull(rows);
        assertEquals("two groups (eng, hr)", 2, rows.size());
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
