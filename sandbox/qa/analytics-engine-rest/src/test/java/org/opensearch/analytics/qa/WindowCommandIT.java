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
        assertEquals("head 3 rows", 3, rows.size());
        // Expected totals: 17 rows over str0 groups. SUM OVER () is the count of rows per str0
        // summed — which is the total row count 17.
        for (int i = 0; i < 3; i++) {
            Object total = rows.get(i).get(2);
            assertTrue("window total should be numeric, got " + total, total instanceof Number);
            assertEquals("SUM(c) OVER () = total row count (17)", 17L, ((Number) total).longValue());
        }
    }

    /**
     * Window over union output: main pipeline with stats, appended with a second stats arm,
     * then eventstats over the unioned result. Each arm's PARTIAL+FINAL aggregate runs per-
     * stage, the Union gathers both at coord (HEP-time ER per arm), and the window sits at
     * coord over the unioned SINGLETON(GATHERED) stream.
     *
     * <p><b>Pending:</b> Volcano hits CannotPlanException — the per-arm SINGLE aggregate is
     * wrapped directly in an ER by OpenSearchUnionRule at HEP time, but AggregateSplitRule
     * never fires because its operand (OpenSearchAggregate) is inside the ER's input subset,
     * not a direct Volcano child of Union. Needs either: (a) lift the aggregate split into
     * HEP too for the per-arm case, or (b) remove the per-arm HEP ER and rely on
     * AggregateSplit + Volcano trait enforcement. Track separately from this PR.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "Window-over-Union with per-arm SINGLE aggregate: AggregateSplit doesn't fire beneath the HEP-wrapped ER.")
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
     * The join's output is SINGLETON(GATHERED) (per-side HEP-time ER + coord join); the
     * window sits at coord, no extra ER needed, cost gate satisfied.
     *
     * <p><b>Pending</b> (same root cause as {@link #testWindowAfterUnion}): per-side
     * {@code SINGLE} aggregate under each HEP-wrapped ER; {@code AggregateSplitRule} never
     * fires because the aggregate sits inside the ER's input RelSet, not a direct Volcano
     * child of Join. Fix shared with the Union case.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(
        bugUrl = "Join-over-Aggregate with per-side HEP-wrapped ER: AggregateSplit doesn't fire beneath the ER. Shared with testWindowAfterUnion."
    )
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
