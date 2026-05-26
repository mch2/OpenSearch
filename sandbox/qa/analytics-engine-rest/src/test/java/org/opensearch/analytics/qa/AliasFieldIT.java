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
 * End-to-end integration test for alias-type fields on parquet-backed indices.
 *
 * <p>Validates that mapping alias fields ({@code "type": "alias", "path": "target"})
 * are queryable through PPL — both in projection and filtering.
 *
 * <p>Requires the SQL plugin (opensearch-sql-plugin) which implements
 * {@code AliasFieldsWrappable} — wrapping a Project above the scan that adds alias
 * columns as identity references to their targets. The test-ppl-frontend does not
 * support alias resolution, so these tests are disabled until the SQL plugin is
 * available in the IT cluster.
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD — alias field "
    + "tests require the SQL plugin (AliasFieldsWrappable). test-ppl-frontend does not support "
    + "alias resolution. Run with opensearch-sql-plugin installed to validate.")
public class AliasFieldIT extends AnalyticsRestTestCase {

    private static final String INDEX = "alias_field_test";

    /**
     * Basic alias projection: {@code source = idx | fields @timestamp}.
     * The alias points to a date field; the projected value should match.
     */
    public void testProjectAliasField() throws Exception {
        createIndex();
        ingestDoc("{\"timestamp\": \"2024-01-15 10:30:00\", \"message\": \"hello\"}");
        flush();

        String ppl = "source = " + INDEX + " | fields @timestamp";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Expected rows in response", rows);
        assertEquals("Should return 1 row", 1, rows.size());
        assertNotNull("@timestamp should be non-null", rows.get(0).get(0));
    }

    /**
     * Filter on alias field: {@code source = idx | where @timestamp > '2024-01-01'}.
     */
    public void testFilterOnAliasField() throws Exception {
        createIndex();
        ingestDoc("{\"timestamp\": \"2024-01-15 10:30:00\", \"message\": \"in range\"}");
        ingestDoc("{\"timestamp\": \"2023-06-01 10:30:00\", \"message\": \"out of range\"}");
        flush();

        String ppl = "source = " + INDEX + " | where @timestamp > '2024-01-01 00:00:00' | fields message";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals("Filter on alias should return 1 row", 1, rows.size());
        assertEquals("in range", rows.get(0).get(0));
    }

    /**
     * Alias and target both queryable in same query.
     */
    public void testAliasAndTargetInSameQuery() throws Exception {
        createIndex();
        ingestDoc("{\"timestamp\": \"2024-03-20 08:00:00\", \"message\": \"test\"}");
        flush();

        String ppl = "source = " + INDEX + " | fields timestamp, @timestamp";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals("Alias and target should return same value",
            rows.get(0).get(0), rows.get(0).get(1));
    }

    /**
     * Aggregation on alias field: {@code stats count() by @timestamp}.
     */
    public void testAggregationOnAliasField() throws Exception {
        createIndex();
        ingestDoc("{\"timestamp\": \"2024-01-15 10:30:00\", \"message\": \"a\"}");
        ingestDoc("{\"timestamp\": \"2024-01-15 10:30:00\", \"message\": \"b\"}");
        ingestDoc("{\"timestamp\": \"2024-01-16 10:30:00\", \"message\": \"c\"}");
        flush();

        String ppl = "source = " + INDEX + " | stats count() as c by @timestamp | sort - c";
        Map<String, Object> result = executePpl(ppl);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull(rows);
        assertEquals("Should have 2 groups", 2, rows.size());
        assertEquals(2, ((Number) rows.get(0).get(0)).intValue());
        assertEquals(1, ((Number) rows.get(1).get(0)).intValue());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String settings = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"timestamp\": {\"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis\"},"
            + "    \"@timestamp\": {\"type\": \"alias\", \"path\": \"timestamp\"},"
            + "    \"message\": {\"type\": \"keyword\"}"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(settings);
        client().performRequest(create);
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
