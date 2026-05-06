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
 * REST integration test for PPL approximate-distinct-count aggregates on the
 * analytics-engine route (DataFusion backend).
 *
 * <p>Three PPL surface forms all canonicalise at parse time (or, for DC, at the
 * grammar level) to the same call:
 *
 * <ul>
 *   <li>{@code distinct_count_approx(x)} — canonical form.</li>
 *   <li>{@code dc(x)} — grammar alias for the same, mapped via the
 *       {@code distinctCountFunctionCall} alt in {@code statsFunction}.</li>
 *   <li>{@code distinct_count(x)} — grammar alias for the same; PPL treats
 *       {@code distinct_count} as approximate (the canonical name for exact
 *       count-distinct in PPL is the {@code COUNT} aggregate with the
 *       DISTINCT modifier, not this token).</li>
 * </ul>
 *
 * <p>All three are accepted by the antlr grammar shipped with the
 * test-ppl-frontend plugin — grammar tokens verified by inspecting
 * {@code OpenSearchPPLParser.distinctCountFunctionCall}
 * ({@code DISTINCT_COUNT | DC | DISTINCT_COUNT_APPROX}). PPL canonicalises all
 * three at resolution time to {@code SqlStdOperatorTable.APPROX_COUNT_DISTINCT},
 * declared here as a {@link org.opensearch.analytics.spi.AggregateFunction#APPROX_COUNT_DISTINCT}
 * approximate capability. Isthmus' default {@code AGGREGATE_SIGS} maps the operator to
 * Substrait core name {@code approx_count_distinct} (from {@code functions_aggregate_approx.yaml}),
 * and the DataFusion substrait consumer maps that to its HyperLogLog built-in.
 *
 * <p>With only 17 rows and these tiny cardinalities, DataFusion's
 * HyperLogLog sketch is exact, so we assert on exact integer equality.
 * The calcs bulk fixture has:
 *
 * <ul>
 *   <li>3 distinct {@code str0} values (FURNITURE, OFFICE SUPPLIES, TECHNOLOGY)</li>
 *   <li>17 distinct {@code str1} values (one per row)</li>
 *   <li>11 distinct {@code int3} values (note: int3 has repeats — 2, 11, 18 each appear multiple times)</li>
 * </ul>
 *
 * <p>Provisions the {@code calcs} dataset (shared with other stat-agg ITs) once per class.
 */
public class DistinctCountApproxCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // NOTE: The canonical {@code distinct_count_approx(x)} surface form currently
    // fails with `Cannot resolve function: DISTINCT_COUNT_APPROX` on this sandbox —
    // the test-ppl-frontend plugin pins a unified-query-core snapshot that doesn't
    // yet register {@code distinct_count_approx} in its parse-time alias table
    // (sql/core's {@code BuiltinFunctionName.AGGREGATION_FUNC_MAPPING}). The two
    // legacy aliases `dc` and `distinct_count` ARE registered, so we exercise the
    // wiring through those forms — same underlying Calcite
    // {@code SqlStdOperatorTable.APPROX_COUNT_DISTINCT} target, same Substrait
    // call, same DataFusion {@code approx_distinct} UDAF.

    // ── dc — grammar alias (exercised across column types) ─────────────────────

    public void testDcOnStr0() throws IOException {
        // 3 distinct str0 categories in the 17-row calcs bulk.
        assertScalarEqualsLong("source=" + DATASET.indexName + " | stats dc(str0) as dc", 3L);
    }

    public void testDcOnStr1() throws IOException {
        // 17 distinct str1 product names — one per row.
        assertScalarEqualsLong("source=" + DATASET.indexName + " | stats dc(str1) as dc", 17L);
    }

    public void testDcOnInt3() throws IOException {
        // Integer field — 11 distinct values among 17 rows (some duplicates: 2, 11, 18).
        assertScalarEqualsLong("source=" + DATASET.indexName + " | stats dc(int3) as dc", 11L);
    }

    // ── distinct_count — grammar alias ──────────────────────────────────────────

    public void testDistinctCountAlias() throws IOException {
        // `distinct_count` is another grammar-accepted alias for the approximate form.
        // Same target as `dc(str0)` above.
        assertScalarEqualsLong("source=" + DATASET.indexName + " | stats distinct_count(str0) as dc", 3L);
    }

    // ── group-by ────────────────────────────────────────────────────────────────

    public void testDcByStr0() throws IOException {
        // Distinct str1 count per str0 group. Exercises partial-agg splitting +
        // final reassembly with HLL intermediate state.
        //   FURNITURE — 2 distinct str1 values (rows 0..1 in the bulk).
        //   OFFICE SUPPLIES — 6 distinct str1 values.
        //   TECHNOLOGY — 9 distinct str1 values.
        assertGroupedScalarEquals(
            "source=" + DATASET.indexName + " | stats dc(str1) as dc by str0",
            Map.of("FURNITURE", 2L, "OFFICE SUPPLIES", 6L, "TECHNOLOGY", 9L)
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private void assertScalarEqualsLong(String ppl, long expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, rows);
        assertEquals("Expected one row for query: " + ppl, 1, rows.size());
        assertEquals("Expected one column for query: " + ppl, 1, rows.get(0).size());
        Object cell = rows.get(0).get(0);
        assertNotNull("Expected non-null aggregate result for query: " + ppl, cell);
        assertTrue("Expected Number result for query: " + ppl + " but got " + cell.getClass(), cell instanceof Number);
        assertEquals("Aggregate value mismatch for query: " + ppl, expected, ((Number) cell).longValue());
    }

    /**
     * Assert a grouped-aggregate query returns exactly the expected group→count
     * mapping. Row order is not compared (DataFusion hash-agg doesn't preserve
     * order). Assumes a 2-column projection: one string group key, one numeric
     * aggregate; column order is inferred at runtime.
     */
    private void assertGroupedScalarEquals(String ppl, Map<String, Long> expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, rows);
        assertEquals("Row count mismatch for query: " + ppl, expected.size(), rows.size());
        for (List<Object> row : rows) {
            assertEquals("Expected 2 columns (group, agg) for query: " + ppl, 2, row.size());
            String key;
            Number valueBox;
            if (row.get(0) instanceof String && row.get(1) instanceof Number) {
                key = (String) row.get(0);
                valueBox = (Number) row.get(1);
            } else if (row.get(0) instanceof Number && row.get(1) instanceof String) {
                key = (String) row.get(1);
                valueBox = (Number) row.get(0);
            } else {
                throw new AssertionError("Unexpected row shape [" + row + "] for query: " + ppl);
            }
            Long expVal = expected.get(key);
            assertNotNull("Unexpected group key [" + key + "] for query: " + ppl, expVal);
            assertEquals("Group [" + key + "] count mismatch for query: " + ppl, expVal.longValue(), valueBox.longValue());
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
