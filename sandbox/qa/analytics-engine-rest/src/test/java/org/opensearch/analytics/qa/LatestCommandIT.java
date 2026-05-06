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
 * REST integration test for PPL {@code latest(field, ts)} — symmetric to
 * {@link EarliestCommandIT}.
 *
 * <p>PPL {@code stats latest(field, ts)} lowers to Calcite
 * {@code SqlStdOperatorTable.ARG_MAX(field, ts)} →
 * {@code NameBasedAggregateFunctionConverter.rewriteArgMinMax} rewrites to
 * substrait {@code last_value(field)} with ORDER BY {@code ts} ASC. DataFusion's
 * native {@code last_value} UDAF returns the last row of the sorted group —
 * i.e. the row with the largest {@code ts}. Equivalent result to
 * {@code first_value(field)} with DESC ordering, chosen for symmetry with
 * DataFusion's own {@code last_value} semantics.
 *
 * <p>Dataset: calcs (17 rows). The absolute latest row by {@code datetime0} is
 * {@code key02 @ 2004-08-02T07:59:23Z} with {@code str1='AIR PURIFIERS'} and
 * {@code num0=15.7}.
 */
public class LatestCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── ungrouped ──────────────────────────────────────────────────────────────

    public void testLatestStringByDatetime() throws IOException {
        // The absolute latest row (by datetime0) is key02 @ 2004-08-02T07:59:23Z
        // with str1='AIR PURIFIERS'.
        assertRows(
            "source=" + DATASET.indexName + " | stats latest(str1, datetime0)",
            row("AIR PURIFIERS")
        );
    }

    public void testLatestNumericByDatetime() throws IOException {
        // Same row (key02) has num0=15.7.
        assertRows(
            "source=" + DATASET.indexName + " | stats latest(num0, datetime0)",
            row(15.7)
        );
    }

    // ── grouped ────────────────────────────────────────────────────────────────

    public void testLatestGroupedByStr0() throws IOException {
        // Per-group latest by datetime0:
        //   FURNITURE       — key01 @ 2004-07-26T12:30:34Z — str1='CLOCKS'
        //   OFFICE SUPPLIES — key02 @ 2004-08-02T07:59:23Z — str1='AIR PURIFIERS'
        //   TECHNOLOGY      — key14 @ 2004-07-31T11:57:52Z — str1='DOT MATRIX PRINTERS'
        assertRows(
            "source=" + DATASET.indexName + " | stats latest(str1, datetime0) by str0 | sort str0",
            row("CLOCKS", "FURNITURE"),
            row("AIR PURIFIERS", "OFFICE SUPPLIES"),
            row("DOT MATRIX PRINTERS", "TECHNOLOGY")
        );
    }

    // ── helpers (same pattern as EarliestCommandIT / StatsCommandIT) ────────────

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
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

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
