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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * REST integration test for PPL standard aggregates (Group E) on the analytics-engine route.
 *
 * <p>Covers the subset of Group E functions that pass end-to-end against {@code main}:
 * <ul>
 *   <li>{@code sum} — PPL {@code stats sum(field)}</li>
 *   <li>{@code count} — {@code count()} (count-star) and {@code count(field)} (null-skipping)</li>
 *   <li>{@code min} / {@code max} — PPL {@code stats min(field) / max(field)}</li>
 * </ul>
 * {@code SUM}, {@code MIN}, {@code MAX}, {@code COUNT} are all declared in
 * {@code DataFusionAnalyticsBackendPlugin.AGG_FUNCTIONS} and wired via isthmus's built-in
 * {@code AGGREGATE_SIGS} → substrait → DataFusion {@code with_default_features}. No Java
 * adapters needed.
 *
 * <p><b>Not covered (blocked on pre-existing issues):</b>
 * <ul>
 *   <li>{@code avg} — declared in {@code AGG_FUNCTIONS} but the isthmus
 *       {@code AggregateFunctionConverter} currently throws
 *       {@code "Unable to find binding for call AVG($0)"} on the analytics-engine route.
 *       Reproducible against {@code upstream/main} without further changes. Needs a separate
 *       bug fix in the substrait converter wiring; once resolved, add {@code testAvg} and
 *       {@code testAvgGroupedByStr0} here.</li>
 *   <li>{@code distinct_count} / {@code dc} — aliased to {@code APPROX_COUNT_DISTINCT} at the
 *       PPL frontend and to DataFusion's HLL-backed {@code approx_distinct} UDAF at the Rust
 *       layer. That approximate path is Group D's responsibility
 *       ({@code distinct_count_approx}); once the UDAF alias + capability declaration land
 *       via that group, the PPL aliases become testable here. The exact form
 *       {@code stats count(DISTINCT field)} is not a PPL grammar production
 *       ({@code OpenSearchPPLParser.g4}'s {@code statsFunction} rule requires one of
 *       {@code DISTINCT_COUNT}, {@code DC}, or {@code DISTINCT_COUNT_APPROX}, not
 *       {@code COUNT DISTINCT}).</li>
 * </ul>
 *
 * <p>Uses the same {@code calcs} dataset as {@link FillNullCommandIT} (17 rows with a mix of
 * doubles, keywords, and integers — many nulls to exercise null-skipping behaviour).
 */
public class StatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Must be called inside a test
     * method (not {@code setUp()}) — {@link org.opensearch.test.rest.OpenSearchRestTestCase}'s
     * static {@code client()} is not initialized until after {@code @BeforeClass}, but is
     * reliably available inside test bodies. Mirrors the pattern in {@code FillNullCommandIT}.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── ungrouped aggregates ────────────────────────────────────────────────────

    public void testSum() throws IOException {
        // num0 non-null values: {12.3, -12.3, 15.7, -15.7, 3.5, -3.5, 0, 10} → sum = 10.0
        assertRows("source=" + DATASET.indexName + " | stats sum(num0)", row(10.0));
    }

    public void testMin() throws IOException {
        assertRows("source=" + DATASET.indexName + " | stats min(num0)", row(-15.7));
    }

    public void testMax() throws IOException {
        assertRows("source=" + DATASET.indexName + " | stats max(num0)", row(15.7));
    }

    public void testCountStar() throws IOException {
        // count() counts every row (17 total).
        assertRows("source=" + DATASET.indexName + " | stats count()", row(17));
    }

    public void testCountField() throws IOException {
        // count(num0) skips nulls → 8.
        assertRows("source=" + DATASET.indexName + " | stats count(num0)", row(8));
    }

    // ── grouped aggregates ──────────────────────────────────────────────────────

    public void testSumGroupedByStr0() throws IOException {
        // str0 partitions num0 (non-null only):
        //   FURNITURE       — {12.3, -12.3}                      sum = 0.0
        //   OFFICE SUPPLIES — {15.7, -15.7, 3.5, -3.5, 0}         sum = 0.0
        //   TECHNOLOGY      — {10}                                sum = 10.0
        assertRows(
            "source=" + DATASET.indexName + " | stats sum(num0) by str0 | sort str0",
            row(0.0, "FURNITURE"),
            row(0.0, "OFFICE SUPPLIES"),
            row(10.0, "TECHNOLOGY")
        );
    }

    public void testCountGroupedByStr0() throws IOException {
        // str0 partitions all 17 rows:
        //   FURNITURE=2, OFFICE SUPPLIES=6, TECHNOLOGY=9
        assertRows(
            "source=" + DATASET.indexName + " | stats count() by str0 | sort str0",
            row(2, "FURNITURE"),
            row(6, "OFFICE SUPPLIES"),
            row(9, "TECHNOLOGY")
        );
    }

    public void testMinGroupedByStr0() throws IOException {
        // Per-group min of num0 (non-null):
        //   FURNITURE       — min(12.3, -12.3)                    = -12.3
        //   OFFICE SUPPLIES — min(15.7, -15.7, 3.5, -3.5, 0)       = -15.7
        //   TECHNOLOGY      — min(10)                              = 10.0
        assertRows(
            "source=" + DATASET.indexName + " | stats min(num0) by str0 | sort str0",
            row(-12.3, "FURNITURE"),
            row(-15.7, "OFFICE SUPPLIES"),
            row(10.0, "TECHNOLOGY")
        );
    }

    public void testMaxGroupedByStr0() throws IOException {
        // Per-group max of num0 (non-null):
        //   FURNITURE       — max(12.3, -12.3)                    = 12.3
        //   OFFICE SUPPLIES — max(15.7, -15.7, 3.5, -3.5, 0)       = 15.7
        //   TECHNOLOGY      — max(10)                              = 10.0
        assertRows(
            "source=" + DATASET.indexName + " | stats max(num0) by str0 | sort str0",
            row(12.3, "FURNITURE"),
            row(15.7, "OFFICE SUPPLIES"),
            row(10.0, "TECHNOLOGY")
        );
    }

    // ── helpers (cloned from FillNullCommandIT — numeric-tolerant row comparator) ──

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }

    /** Send {@code POST /_analytics/ppl} and return the parsed JSON body. */
    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Compare two cells with numeric tolerance. JSON parsing produces Integer/Long/Double
     * values that may not match {@code .equals()} across types even when numerically equal;
     * treat any two {@link Number} instances as equal if their {@code double} values compare
     * equal. Falls back to {@link java.util.Objects#equals} otherwise.
     */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }
}
