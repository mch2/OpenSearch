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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Joins + window functions + group-by on multi-shard indices.
 *
 * <p>Aggregations are sum/count/min/max only — avg/stddev/var hit an upstream
 * nested-window bug (see {@link StreamstatsCommandIT}).
 *
 * <p>Calcs dataset (17 rows): FURNITURE=2, OFFICE SUPPLIES=6, TECHNOLOGY=9.
 * Provisioned at 3 shards into two indices so every test stresses multi-shard cuts.
 *
 * <p>Joins project to int/keyword fields first — Arrow conversion rejects TIMESTAMP/DATE.
 */
public class JoinWindowIntegrationIT extends AnalyticsRestTestCase {

    private static final Dataset CALCS = new Dataset("calcs", "calcs_multi_shard");
    private static final Dataset CALCS_ALT = new Dataset("calcs", "calcs_alt_multi_shard");
    private static final int SHARDS = 3;

    private static final Set<String> PROVISIONED = new HashSet<>();

    private void ensureDataProvisioned() throws IOException {
        if (PROVISIONED.add(CALCS.indexName)) {
            DatasetProvisioner.provision(client(), CALCS, SHARDS);
        }
        if (PROVISIONED.add(CALCS_ALT.indexName)) {
            DatasetProvisioner.provision(client(), CALCS_ALT, SHARDS);
        }
    }

    // ── Multi-aggregation in a single stats clause ────────────────────────────

    /** Multi-agg (sum + count + max) on each side, then inner join on the group key. 3 rows. */
    public void testMultiAggBeforeJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats sum(int0) as s, count() as c, max(int0) as m by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as c2 by str0 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    /** Inner join, then multi-agg (sum + count + min) by str0. Joined rows reduce to 3 groups. */
    public void testMultiAggAfterJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | fields key, int0, str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | fields key, str0 ]"
            + " | stats sum(int0) as s, count() as c, min(int0) as m by str0"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    // ── GROUP BY shapes ───────────────────────────────────────────────────────

    /** Diagnostic for Bug B: per-group count on multi-shard, no join. Should be FURNITURE=2, OFFICE SUPPLIES=6, TECHNOLOGY=9. */
    public void testStatsCountByStr0_multiShard_diagnostic() throws IOException {
        String ppl = "source=" + CALCS.indexName + " | stats count() as c by str0 | fields str0, c";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        logger.info("Diagnostic response rows: {}", rows);
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("3 distinct str0 groups", 3, rows.size());
        Map<String, Long> actual = new HashMap<>();
        for (List<Object> row : rows) {
            actual.put((String) row.get(0), ((Number) row.get(1)).longValue());
        }
        assertEquals("FURNITURE count", 2L, (long) actual.get("FURNITURE"));
        assertEquals("OFFICE SUPPLIES count", 6L, (long) actual.get("OFFICE SUPPLIES"));
        assertEquals("TECHNOLOGY count", 9L, (long) actual.get("TECHNOLOGY"));
    }


    /** Diagnostic: SUM by group — works for SUM regardless of phase since sum-of-partial-sums = total sum. */
    public void testStatsSumByStr0_multiShard_diagnostic() throws IOException {
        String ppl = "source=" + CALCS.indexName + " | stats sum(int0) as s by str0 | fields str0, s";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        logger.info("Diagnostic SUM response rows: {}", rows);
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        Map<String, Long> actual = new HashMap<>();
        for (List<Object> row : rows) {
            actual.put((String) row.get(0), ((Number) row.get(1)).longValue());
        }
        assertEquals("FURNITURE sum int0", 1L, (long) actual.get("FURNITURE"));
        assertEquals("OFFICE SUPPLIES sum int0", 18L, (long) actual.get("OFFICE SUPPLIES"));
        assertEquals("TECHNOLOGY sum int0", 49L, (long) actual.get("TECHNOLOGY"));
    }

    /** Both sides grouped by str0 (3 groups each), then inner-joined. Asserts per-group counts. */
    public void testJoinOnGroupedData_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats count() as left_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as right_cnt by str0 ]"
            + " | fields str0, left_cnt, right_cnt";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("3 str0 groups joined to themselves", 3, rows.size());

        // Collect into a map so the test doesn't depend on response row order.
        Map<String, long[]> byStr0 = new HashMap<>();
        for (List<Object> row : rows) {
            byStr0.put((String) row.get(0), new long[] { ((Number) row.get(1)).longValue(), ((Number) row.get(2)).longValue() });
        }
        Map<String, Long> expected = Map.of("FURNITURE", 2L, "OFFICE SUPPLIES", 6L, "TECHNOLOGY", 9L);
        for (Map.Entry<String, Long> e : expected.entrySet()) {
            long[] counts = byStr0.get(e.getKey());
            assertNotNull("Missing group " + e.getKey(), counts);
            assertEquals("left_cnt for " + e.getKey(), e.getValue().longValue(), counts[0]);
            assertEquals("right_cnt for " + e.getKey(), e.getValue().longValue(), counts[1]);
        }
    }

    /** Group by two keys (str0, bool2) on each side, inner-join on both. 5 groups. */
    @AwaitsFix(bugUrl = "composite-key group-by feeding a join hangs in execution. Right-side stage with Sort(fetch=50000) over composite-key agg never completes — happens for both COUNT and SUM aggregates, so not COUNT-specific. Same shape with single-key works.")
    public void testJoinOnTwoGroupKeys_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats count() as left_cnt by str0, bool2"
            + " | inner join left=a, right=b ON a.str0 = b.str0 AND a.bool2 = b.bool2"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as right_cnt by str0, bool2 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 5L);
    }

    // ── HAVING-equivalent (filter after stats) ────────────────────────────────

    /** Filter on a stats-derived column after a join (PPL's HAVING). 2 groups pass left_cnt > 4. */
    public void testHavingFilterAfterJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats count() as left_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as right_cnt by str0 ]"
            + " | where left_cnt > 4"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 2L);
    }

    // ── Nested join (3-table) ─────────────────────────────────────────────────

    /** Three-way inner join. Fires the join rule twice, two cuts in the DAG. 3 groups match. */
    public void testNestedJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats count() as a_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as b_cnt by str0 ]"
            + " | inner join left=ab, right=c ON ab.str0 = c.str0"
            + " [ source=" + CALCS.indexName + " | stats count() as c_cnt by str0 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 3L);
    }

    // ── Window functions on join output ───────────────────────────────────────

    /** Running count over a join's output. Multi-shard: must be a global count, not per-shard. */
    public void testStreamstatsAfterJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | fields key, int0, str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | fields key, str0 ]"
            + " | sort str0"
            + " | streamstats count() as running_count"
            + " | fields running_count"
            + " | head 3";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("running_count at row " + i, (long) (i + 1), ((Number) rows.get(i).get(0)).longValue());
        }
    }

    /** Global max(int0) over a join's output. Every row sees the same global value (11). */
    public void testEventstatsAfterJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | fields key, int0, str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | fields key, str0 ]"
            + " | eventstats max(int0) as global_max"
            + " | fields global_max"
            + " | head 3";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("global_max at row " + i, 11, ((Number) rows.get(i).get(0)).intValue());
        }
    }

    /** eventstats output feeding a join. Two gathers in a row — both reducers must cut cleanly. */
    public void testEventstatsThenJoin_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | fields key, int0, str0"
            + " | eventstats max(int0) as global_max"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | fields key, str0 ]"
            + " | stats count() as cnt";
        assertSingleCount(ppl, 121L);
    }

    /** Full pipeline: stats → join → sort → streamstats. Running sum after sort: 2, 8, 17. */
    @AwaitsFix(bugUrl = "streamstats over a join's output ignores the upstream sort — running sum follows arrival order")
    public void testStatsJoinStreamstats_multiShard() throws IOException {
        String ppl = "source=" + CALCS.indexName
            + " | stats count() as left_cnt by str0"
            + " | inner join left=a, right=b ON a.str0 = b.str0"
            + " [ source=" + CALCS_ALT.indexName + " | stats count() as right_cnt by str0 ]"
            + " | sort str0"
            + " | streamstats sum(left_cnt) as running_left"
            + " | fields str0, running_left";
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        logger.info("testStatsJoinStreamstats response rows: {}", rows);
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("3 str0 groups", 3, rows.size());
        String[] expectedStr0 = { "FURNITURE", "OFFICE SUPPLIES", "TECHNOLOGY" };
        long[] expectedRunning = { 2, 8, 17 };
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("str0 at row " + i, expectedStr0[i], rows.get(i).get(0));
            assertEquals("running_left at row " + i, expectedRunning[i], ((Number) rows.get(i).get(1)).longValue());
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private void assertSingleCount(String ppl, long expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Expected single count row for query: " + ppl, 1, rows.size());
        Object actual = rows.get(0).get(0);
        assertTrue("Expected numeric count for query: " + ppl + " but got: " + actual, actual instanceof Number);
        assertEquals("Count mismatch for query: " + ppl, expected, ((Number) actual).longValue());
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
