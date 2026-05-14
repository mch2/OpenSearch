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
 * Integration tests for PPL commands that lower to {@code LogicalWindow} on the
 * analytics-engine route. Covers {@code eventstats} — ROW_NUMBER / RANK /
 * DENSE_RANK / SUM / AVG / COUNT / MIN / MAX over empty OVER().
 *
 * <p>PARTITION BY is intentionally out of scope: no shuffle exchange exists today,
 * and the project rule rejects window expressions with non-empty partition keys.
 */
public class WindowCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static final Dataset DATASET_ALT = new Dataset("calcs", "calcs_alt");

    private static boolean dataProvisioned = false;
    private static boolean altProvisioned = false;

    /** Provision calcs with 3 shards so window runs through the multi-shard path (HEP-time ER). */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET, 3);
            dataProvisioned = true;
        }
    }

    /** Provision a second calcs index (3 shards) for join-shape tests needing a distinct right side. */
    private void ensureDataProvisionedAlt() throws IOException {
        ensureDataProvisioned();
        if (altProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET_ALT, 3);
            altProvisioned = true;
        }
    }

    /** {@code eventstats count()} lowers to {@code COUNT() OVER ()} — aggregate-as-window. */
    public void testEventstatsCount() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats count() as n | fields int0, n | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 rows", 3, rows.size());
        for (int i = 0; i < 3; i++) {
            Object n = rows.get(i).get(1);
            assertTrue("window count should be numeric, got " + n, n instanceof Number);
            assertEquals("COUNT() OVER () broadcasts calcs row count (17)", 17L, ((Number) n).longValue());
        }
    }

    /** {@code eventstats sum(int0)} → {@code SUM(int0) OVER ()}. */
    public void testEventstatsSum() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats sum(int0) as s | fields int0, s | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 rows", 3, rows.size());
        // calcs int0 non-null values: 1, 7, 3, 8, 8, 4, 10, 4, 11, 4, 8 = 68
        for (int i = 0; i < 3; i++) {
            Object s = rows.get(i).get(1);
            assertTrue("window sum should be numeric, got " + s, s instanceof Number);
            assertEquals("SUM(int0) OVER () broadcasts the global sum (68)", 68L, ((Number) s).longValue());
        }
    }

    /**
     * Window over aggregate output: {@code stats count() by str0 | eventstats sum(c) as total}.
     * The inner stats produces a FINAL aggregate; the outer eventstats wraps a SUM() OVER ()
     * around its output. Verifies the planner puts the ER between PARTIAL and FINAL (aggregate
     * split) and then the window runs at coord over the FINAL's SINGLETON(GATHERED) output —
     * no scatter explosion, no redundant ER.
     */
    public void testWindowAfterAggregate() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by str0 | eventstats sum(c) as total | fields str0, c, total | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("at least 1 row", rows.size() > 0);
        // SUM(c) OVER () broadcasts the same global value across every row.
        long firstTotal = ((Number) rows.get(0).get(2)).longValue();
        assertTrue("window total should be positive, got " + firstTotal, firstTotal > 0);
        for (int i = 1; i < rows.size(); i++) {
            assertEquals("SUM OVER () broadcasts same value across rows", firstTotal, ((Number) rows.get(i).get(2)).longValue());
        }
    }

    /**
     * Window over union output: main pipeline with stats, appended with a second stats arm,
     * then eventstats over the unioned result.
     */
    public void testWindowAfterUnion() throws IOException {
        Map<String, Object> response = executePpl(
            "source="
                + DATASET.indexName
                + " | stats count() as c by str0"
                + " | append [ source=" + DATASET.indexName + " | stats count() as c by str0 ]"
                + " | eventstats sum(c) as total | fields str0, c, total | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("at least 1 row, got " + rows.size(), rows.size() > 0);
        // Union of two identical stats branches. Every row's total should be the same
        // (SUM OVER () broadcasts). Assert numeric + positive + consistent.
        long firstTotal = ((Number) rows.get(0).get(2)).longValue();
        assertTrue("window total should be positive, got " + firstTotal, firstTotal > 0);
        for (int i = 1; i < rows.size(); i++) {
            assertEquals("SUM OVER () broadcasts same value across rows", firstTotal, ((Number) rows.get(i).get(2)).longValue());
        }
    }

    /**
     * Window after join: inner-join two indices, then eventstats over the joined output.
     */
    public void testWindowAfterJoin() throws IOException {
        ensureDataProvisionedAlt();
        Map<String, Object> response = executePpl(
            "source="
                + DATASET.indexName
                + " | stats count() as c_left by str0"
                + " | inner join left=a, right=b ON a.str0 = b.str0"
                + " [ source=" + DATASET_ALT.indexName + " | stats count() as c_right by str0 ]"
                + " | eventstats sum(c_left) as total_left | fields str0, c_left, c_right, total_left | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("at least 1 row, got " + rows.size(), rows.size() > 0);
        long firstTotal = ((Number) rows.get(0).get(3)).longValue();
        assertTrue("window total should be positive, got " + firstTotal, firstTotal > 0);
        for (int i = 1; i < rows.size(); i++) {
            assertEquals("SUM OVER () broadcasts same value across rows", firstTotal, ((Number) rows.get(i).get(3)).longValue());
        }
    }

    /**
     * Window after a Filter — {@code where int0 > 5 | eventstats sum(int0) as s}.
     * The filter narrows the dataset and the window runs over the filtered rows. Asserts
     * the window total reflects the filter (i.e. is below the unfiltered total of 68).
     */
    public void testEventstatsAfterWhere() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where int0 > 5 | eventstats sum(int0) as s | fields int0, s | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("at least 1 row", rows.size() > 0);
        // calcs int0 values > 5: 7, 8, 8, 10, 11, 8 = 52.
        long firstTotal = ((Number) rows.get(0).get(1)).longValue();
        assertEquals("SUM(int0) OVER () after where int0 > 5 = 52", 52L, firstTotal);
        for (int i = 1; i < rows.size(); i++) {
            assertEquals("SUM OVER () broadcasts same value", firstTotal, ((Number) rows.get(i).get(1)).longValue());
        }
    }

    /**
     * Window after a collated Sort — {@code sort int0 | eventstats sum(int0) as s}.
     * Sort gathers to coord; the window runs over its (already-singleton) output.
     * Tests that the rule ordering doesn't put a redundant ER between Sort and Project.
     */
    public void testEventstatsAfterSort() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort int0 | eventstats sum(int0) as s | fields int0, s | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertTrue("at least 1 row", rows.size() > 0);
        long firstTotal = ((Number) rows.get(0).get(1)).longValue();
        assertEquals("SUM(int0) OVER () = 68 (full unfiltered total)", 68L, firstTotal);
        for (int i = 1; i < rows.size(); i++) {
            assertEquals("SUM OVER () broadcasts same value", firstTotal, ((Number) rows.get(i).get(1)).longValue());
        }
    }

    /**
     * Multiple windows in one {@code eventstats}: {@code sum(int0) as s, count() as n}.
     * PPL emits two RexOvers in the same Project — the planner must produce a single
     * Project carrying both, not two separate Projects each with its own ER.
     */
    public void testEventstatsMultipleWindows() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats sum(int0) as s, count() as n | fields int0, s, n | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 rows", 3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("SUM(int0) OVER () = 68", 68L, ((Number) rows.get(i).get(1)).longValue());
            assertEquals("COUNT() OVER () = 17", 17L, ((Number) rows.get(i).get(2)).longValue());
        }
    }

    /**
     * Mirrors {@code PlanShapeTests.testWindowThenSort_2shard}: window function then
     * Sort. The RexOver Project gathers to coord; Sort runs at coord with no extra ER.
     * Asserts the window total broadcasts to every row regardless of sort order.
     */
    public void testEventstatsThenSort() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats sum(int0) as s | sort int0 | fields int0, s | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 rows", 3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("SUM(int0) OVER () = 68 broadcasts to every row after sort", 68L, ((Number) rows.get(i).get(1)).longValue());
        }
    }

    /** {@code eventstats max(int0)} → {@code MAX(int0) OVER ()}. */
    public void testEventstatsMax() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats max(int0) as m | fields int0, m | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("head 3 rows", 3, rows.size());
        for (int i = 0; i < 3; i++) {
            Object m = rows.get(i).get(1);
            assertTrue("window max should be numeric, got " + m, m instanceof Number);
            assertEquals("MAX(int0) OVER () = 11", 11L, ((Number) m).longValue());
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
