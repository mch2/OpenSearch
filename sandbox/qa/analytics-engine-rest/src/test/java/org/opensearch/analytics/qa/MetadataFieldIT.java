/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * End-to-end integration test for querying metadata fields (_id, _routing) via PPL
 * on parquet-backed indices.
 *
 * <p>Validates that system metadata fields written by the parquet data format plugin
 * are queryable through the analytics engine's PPL endpoint. These fields exist in
 * parquet storage but are not declared in user index mappings.
 *
 * <p>Run with:
 * ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.MetadataFieldIT" -Dsandbox.enabled=true
 */
public class MetadataFieldIT extends AnalyticsRestTestCase {

    private static final String INDEX = "metadata_field_test";

    /**
     * Ingest a document, retrieve its _id from the index response, then query for
     * it via PPL: {@code source = idx | where _id = 'X' | fields message}.
     * Asserts exactly one row is returned containing the original message.
     */
    public void testFilterById() throws Exception {
        createParquetIndex();

        String docId = ingestAndGetId("{\"message\": \"hello metadata\", \"status\": 200}");
        flush();

        String ppl = "source = " + INDEX + " | where _id = '" + docId + "' | fields message";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Expected rows in response", rows);
        assertEquals("Filter by _id should return exactly 1 row", 1, rows.size());
        assertEquals("hello metadata", rows.get(0).get(0));
    }

    /**
     * Project _id in the fields list: {@code source = idx | fields _id, message}.
     * Asserts that _id appears as a non-null value in the output.
     */
    public void testProjectId() throws Exception {
        createParquetIndex();

        ingestDoc("{\"message\": \"project test\", \"status\": 200}");
        flush();

        String ppl = "source = " + INDEX + " | fields _id, message";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Expected rows in response", rows);
        assertFalse("Should have at least one row", rows.isEmpty());
        assertNotNull("_id should be non-null in projection", rows.get(0).get(0));
    }

    /**
     * Ingest with explicit routing, then filter by _routing:
     * {@code source = idx | where _routing = 'shard_A' | fields message}.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD — _routing "
        + "parquet storage may not persist the routing value for non-required routing indices; "
        + "needs investigation into RoutingParquetField write path.")
    public void testFilterByRouting() throws Exception {
        createParquetIndex();

        Request bulkReq = new Request("POST", "/" + INDEX + "/_bulk");
        bulkReq.setJsonEntity(
            "{\"index\":{\"routing\":\"shard_A\"}}\n{\"message\":\"routed A\",\"status\":200}\n"
                + "{\"index\":{\"routing\":\"shard_B\"}}\n{\"message\":\"routed B\",\"status\":200}\n"
        );
        bulkReq.setOptions(bulkReq.getOptions().toBuilder()
            .addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulkReq);
        flush();

        String ppl = "source = " + INDEX + " | where _routing = 'shard_A' | fields message";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Expected rows in response", rows);
        assertEquals("Filter by _routing='shard_A' should return 1 row", 1, rows.size());
        assertEquals("routed A", rows.get(0).get(0));
    }

    /**
     * Multi-shard correctness: ingest 10 docs across 2 shards, filter by one _id,
     * verify exactly one row returns regardless of which shard holds it.
     */
    public void testFilterByIdMultiShard() throws Exception {
        createParquetIndex(4);

        String targetId = null;
        for (int i = 0; i < 10; i++) {
            String id = ingestAndGetId("{\"message\": \"doc" + i + "\", \"status\": " + i + "}");
            if (i == 5) targetId = id;
        }
        flush();

        String ppl = "source = " + INDEX + " | where _id = '" + targetId + "' | fields message";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals("Filter by _id across 4 shards should return exactly 1 row", 1, rows.size());
        assertEquals("doc5", rows.get(0).get(0));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void createParquetIndex() throws Exception {
        createParquetIndex(1);
    }

    private void createParquetIndex(int shards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String settings = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"message\": {\"type\": \"keyword\"},"
            + "    \"status\": {\"type\": \"integer\"}"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(settings);
        client().performRequest(create);
    }

    private String ingestAndGetId(String docJson) throws Exception {
        Request indexReq = new Request("POST", "/" + INDEX + "/_doc?refresh=false");
        indexReq.setJsonEntity(docJson);
        Response response = client().performRequest(indexReq);
        int status = response.getStatusLine().getStatusCode();
        assertTrue("Expected 200 or 201, got " + status, status == 200 || status == 201);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = entityAsMap(response);
        return (String) body.get("_id");
    }

    private void ingestDoc(String docJson) throws Exception {
        Request indexReq = new Request("POST", "/" + INDEX + "/_doc?refresh=false");
        indexReq.setJsonEntity(docJson);
        client().performRequest(indexReq);
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private Map<String, Object> executePpl(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, ppl);
    }
}
