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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared base for sandbox/qa integration tests that <em>reproduce</em> the upstream
 * {@code org.opensearch.sql.calcite.remote.*} (and a few {@code org.opensearch.sql.ppl.*})
 * tests against the analytics-engine route (parquet primary + lucene secondary, forced via
 * {@code cluster.pluggable.dataformat=composite}).
 *
 * <p>Why this exists: the upstream tests assert against {@code org.json} responses through a
 * Hamcrest {@code MatcherUtils} surface ({@code verifySchema(schema(name,type)...)},
 * {@code verifyDataRows(rows(...))}). Our sandbox suite speaks {@code Map}-based responses from
 * {@link AnalyticsRestTestCase#executePpl}. This base reimplements just enough of that matcher
 * surface — {@link #schema}, {@link #rows}, {@link #verifySchema}, {@link #verifyDataRows},
 * {@link #verifySchemaInOrder}, {@link #verifyDataRowsInOrder}, {@link #verifyErrorMessageContains}
 * — so a failing upstream test body can be ported near-verbatim, and a divergence in the
 * analytics-engine path reproduces here as the same assertion failure.
 *
 * <p>Type labels match the opensearch-sql wire schema ({@code bigint}, {@code int}, {@code string},
 * {@code timestamp}, {@code double}, {@code float}, {@code boolean}, {@code ip}, ...). The
 * comparison is value-based with numeric tolerance, mirroring {@code JSONArray.similar} plus the
 * {@code closeTo} ULP slack.
 */
public abstract class CalciteReproTestCase extends AnalyticsRestTestCase {

    // ── index provisioning ──────────────────────────────────────────────────

    /**
     * Create a parquet-primary / lucene-secondary index with the given mappings block
     * (the {@code {"properties": {...}}} object, WITHOUT the enclosing {@code mappings} key).
     * Swallows resource_already_exists so reruns against a preserved cluster are idempotent.
     */
    protected void createParquetIndex(String name, String mappingsProperties) throws IOException {
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\","
                + "\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":" + mappingsProperties + "}}"
        );
        try {
            client().performRequest(create);
        } catch (ResponseException re) {
            String body = entityAsString(re.getResponse());
            if (body.contains("resource_already_exists_exception") == false) {
                throw re;
            }
        }
    }

    /** Index a single document (refresh=true) at an explicit id. */
    protected void indexDoc(String index, String id, String jsonDoc) throws IOException {
        Request put = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
        put.setJsonEntity(jsonDoc);
        client().performRequest(put);
    }

    /** Bulk-index NDJSON docs (one JSON object per line, no action lines — they're injected). */
    protected void bulkDocs(String index, String ndjsonDocs) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (String doc : ndjsonDocs.split("\n")) {
            if (doc.isBlank()) continue;
            bulk.append("{\"index\": {}}\n").append(doc).append("\n");
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "bulk " + index);
        assertEquals("bulk into " + index + " had errors", false, response.get("errors"));
    }

    protected void deleteIndexQuietly(String name) {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {
            // index may not exist
        }
    }

    // ── response access ──────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    protected static List<Map<String, Object>> schemaOf(Map<String, Object> response) {
        Object schema = response.get("schema");
        assertNotNull("Response missing 'schema': " + response, schema);
        return (List<Map<String, Object>>) schema;
    }

    @SuppressWarnings("unchecked")
    protected static List<List<Object>> dataRowsOf(Map<String, Object> response) {
        Object rows = response.get("datarows");
        assertNotNull("Response missing 'datarows': " + response, rows);
        return (List<List<Object>>) rows;
    }

    // ── matcher surface (mirrors opensearch-sql MatcherUtils) ─────────────────

    /** A (name, type) schema-column expectation. */
    protected static final class SchemaCol {
        final String name;
        final String type;
        SchemaCol(String name, String type) { this.name = name; this.type = type; }
        @Override public String toString() { return "(name=" + name + ", type=" + type + ")"; }
    }

    protected static SchemaCol schema(String name, String type) {
        return new SchemaCol(name, type);
    }

    protected static List<Object> rows(Object... values) {
        return Arrays.asList(values);
    }

    /** Order-insensitive schema match (mirrors verifySchema → containsInAnyOrder). */
    protected void verifySchema(Map<String, Object> response, SchemaCol... expected) {
        List<Map<String, Object>> actual = schemaOf(response);
        assertEquals("schema column count; actual=" + actual, expected.length, actual.size());
        List<String> actualLabels = new ArrayList<>();
        for (Map<String, Object> c : actual) {
            actualLabels.add(c.get("name") + ":" + c.get("type"));
        }
        for (SchemaCol col : expected) {
            String want = col.name + ":" + col.type;
            assertTrue("schema missing column " + want + "; actual=" + actualLabels,
                actualLabels.remove(want));
        }
    }

    /** Order-sensitive schema match (mirrors verifySchemaInOrder → contains). */
    protected void verifySchemaInOrder(Map<String, Object> response, SchemaCol... expected) {
        List<Map<String, Object>> actual = schemaOf(response);
        assertEquals("schema column count; actual=" + actual, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals("schema name at " + i, expected[i].name, actual.get(i).get("name"));
            assertEquals("schema type at " + i + " (" + expected[i].name + ")",
                expected[i].type, actual.get(i).get("type"));
        }
    }

    /** Order-insensitive datarows match (mirrors verifyDataRows → containsInAnyOrder). */
    @SafeVarargs
    @SuppressWarnings("varargs")
    protected final void verifyDataRows(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actual = new ArrayList<>(dataRowsOf(response));
        assertEquals("row count; actual=" + actual, expected.length, actual.size());
        for (List<Object> want : expected) {
            int found = -1;
            for (int i = 0; i < actual.size(); i++) {
                if (rowsEqual(want, actual.get(i))) { found = i; break; }
            }
            assertTrue("expected row " + want + " not found in remaining " + actual, found >= 0);
            actual.remove(found);
        }
    }

    /** Order-sensitive datarows match (mirrors verifyDataRowsInOrder → contains). */
    @SafeVarargs
    @SuppressWarnings("varargs")
    protected final void verifyDataRowsInOrder(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actual = dataRowsOf(response);
        assertEquals("row count; actual=" + actual, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertTrue("row " + i + " mismatch: expected " + expected[i] + " but was " + actual.get(i),
                rowsEqual(expected[i], actual.get(i)));
        }
    }

    protected void verifyNumOfRows(Map<String, Object> response, int n) {
        assertEquals("row count", n, dataRowsOf(response).size());
    }

    /** At-least-one-row match (mirrors verifyDataRowsSome → hasItems). */
    protected void verifyDataRowsSome(Map<String, Object> response, List<Object> expected) {
        List<List<Object>> actual = dataRowsOf(response);
        for (List<Object> got : actual) {
            if (rowsEqual(expected, got)) {
                return;
            }
        }
        fail("expected at least one row matching " + expected + " but rows were " + actual);
    }

    private static boolean rowsEqual(List<Object> want, List<Object> got) {
        if (want.size() != got.size()) return false;
        for (int j = 0; j < want.size(); j++) {
            if (!cellEquals(want.get(j), got.get(j))) return false;
        }
        return true;
    }

    private static boolean cellEquals(Object expected, Object actual) {
        if (expected == null || actual == null) return expected == actual;
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            double diff = Math.abs(e - a);
            double tol = Math.max(1e-9, 4.0 * Math.max(Math.ulp(e), Math.ulp(a)));
            return diff <= tol;
        }
        // Nested structures (objects/arrays) arrive as Map/List from JSON parsing.
        return expected.equals(actual);
    }

    // ── failure-path helpers ──────────────────────────────────────────────────

    /** Execute PPL expecting an HTTP error; returns the response body string for assertions. */
    protected String executePplExpectingFailure(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            fail("Expected failure but got: " + assertOkAndParse(response, ppl));
            return ""; // unreachable
        } catch (ResponseException re) {
            return entityAsString(re.getResponse());
        }
    }

    protected static void verifyErrorMessageContains(String errorBody, String msg) {
        assertTrue("expected error to contain [" + msg + "] but was: " + errorBody,
            errorBody.contains(msg));
    }

    protected static String entityAsString(Response response) throws IOException {
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // ── one-shot lazy provisioning guard ──────────────────────────────────────

    /** Subclasses set this to track per-JVM provisioning. */
    private final Map<String, Boolean> provisionFlags = new LinkedHashMap<>();

    protected boolean firstTime(String key) {
        if (provisionFlags.containsKey(key)) return false;
        provisionFlags.put(key, true);
        return true;
    }
}
