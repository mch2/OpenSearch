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
 * REST integration test for PPL {@code earliest(field, ts)} on the analytics-engine route.
 *
 * <p>PPL's {@code stats earliest(field, ts)} lowers to Calcite
 * {@code SqlStdOperatorTable.ARG_MIN(field, ts)} at the PPL frontend layer
 * (via {@code PPLFuncImpTable.resolveTimeField} +
 * {@code UserDefinedFunctionUtils.makeAggregateCall}). DataFusion 52.x has no
 * native {@code arg_min} / {@code min_by} UDAF, so
 * {@code NameBasedAggregateFunctionConverter.rewriteArgMinMax} transforms the
 * substrait emission to {@code first_value(field)} with an ORDER BY {@code ts}
 * ASC sort field; DataFusion's native {@code first_value} UDAF resolves by name
 * at the substrait consumer and picks the row with the smallest {@code ts}.
 *
 * <p>Dataset: calcs (17 rows). The {@code datetime0} timestamp column is non-null
 * for every row; the {@code str1} value column is non-null for every row. Ordering
 * the 17 rows by {@code datetime0} ascending places {@code key=key08,
 * str1='ANSWERING MACHINES'} first — the expected result of
 * {@code earliest(str1, datetime0)} ungrouped.
 */
public class EarliestCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── ungrouped ──────────────────────────────────────────────────────────────

    public void testEarliestStringByDatetime() throws IOException {
        // The absolute earliest row (by datetime0) is key08 @ 2004-07-04T22:49:28Z
        // with str1='ANSWERING MACHINES'.
        assertRows(
            "source=" + DATASET.indexName + " | stats earliest(str1, datetime0)",
            row("ANSWERING MACHINES")
        );
    }

    public void testEarliestNumericByDatetime() throws IOException {
        // Same earliest row (key08) has num0=10.
        assertRows(
            "source=" + DATASET.indexName + " | stats earliest(num0, datetime0)",
            row(10)
        );
    }

    // ── grouped ────────────────────────────────────────────────────────────────

    public void testEarliestGroupedByStr0() throws IOException {
        // Per-group earliest by datetime0:
        //   FURNITURE       — key00 @ 2004-07-09T10:17:35Z — str1='CLAMP ON LAMPS'
        //   OFFICE SUPPLIES — key03 @ 2004-07-05T13:14:20Z — str1='BINDER ACCESSORIES'
        //   TECHNOLOGY      — key08 @ 2004-07-04T22:49:28Z — str1='ANSWERING MACHINES'
        // Sort by str0 for deterministic row order.
        assertRows(
            "source=" + DATASET.indexName + " | stats earliest(str1, datetime0) by str0 | sort str0",
            row("CLAMP ON LAMPS", "FURNITURE"),
            row("BINDER ACCESSORIES", "OFFICE SUPPLIES"),
            row("ANSWERING MACHINES", "TECHNOLOGY")
        );
    }

    // ── helpers (same pattern as FillNullCommandIT / StatsCommandIT) ────────────

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
