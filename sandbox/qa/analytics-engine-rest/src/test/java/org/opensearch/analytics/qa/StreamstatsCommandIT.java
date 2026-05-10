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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end integration test for PPL {@code streamstats sum(...)} on the analytics-engine route.
 *
 * <p>Exercises the smallest window-function shape end-to-end — a running sum over an
 * integer column — to prove the window capability track: SPI {@code WindowFunction} +
 * {@code WindowFunctionCapability}, {@code OpenSearchProjectRule}'s {@code RexOver}
 * annotation branch, {@code CapabilityRegistry}'s {@code windowBackendsAnyFormat} lookup,
 * and the pre-existing isthmus {@code WindowFunctionConverter} wiring in
 * {@code DataFusionFragmentConvertor}. streamstats with no {@code by} or
 * {@code window=}/reset clauses lowers to a single {@code Project(RexOver(SUM, … OVER
 * ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))} — no {@code LogicalWindow} rel,
 * no partitioning, no sliding frame.
 *
 * <p>Other streamstats shapes (by-groups, reset, window-N sliding) depend on additional
 * relational machinery — {@code Correlate}+{@code Aggregate} for by-groups, a second
 * {@code ROW_NUMBER} window for global sequence — which this MVP does not yet support.
 * Add those alongside the matching capability entries in a follow-up PR.
 */
public class StreamstatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static final Dataset DATASET_MULTI_SHARD = new Dataset("calcs", "calcs_multi_shard");
    private static final int MULTI_SHARD_COUNT = 3;

    private static final Set<String> PROVISIONED = new HashSet<>();

    private void ensureDataProvisioned() throws IOException {
        ensureProvisioned(DATASET, 0);
    }

    private void ensureProvisioned(Dataset dataset, int numberOfShards) throws IOException {
        if (PROVISIONED.add(dataset.indexName)) {
            DatasetProvisioner.provision(client(), dataset, numberOfShards);
        }
    }

    /**
     * Running-sum over {@code int0}. {@code sort key} upstream pins a deterministic scan
     * order so the cumulative sum is checkable. Only the first 5 rows are inspected — a
     * small window keeps the assertion self-contained without making the test brittle to
     * the full dataset size.
     *
     * <p>calcs rows sorted by {@code key} ASC start with: key00=1, key01=null, key02=null, key03=null, key04=7.
     * SUM ignores nulls, so the running sum is: 1, 1, 1, 1, 8.
     */
    public void testStreamstatsRunningSumOverInteger() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats sum(int0) as running_sum | fields key, int0, running_sum | head 5"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected 5 rows from head 5", 5, rows.size());

        // Row-by-row assertions pin the SUM-ignores-null behavior that makes this test
        // interesting end-to-end: a naive SUM that treated nulls as zero would return the
        // same values here, but a broken window frame (e.g. only current row) would surface
        // as running_sum following int0 rather than accumulating.
        Integer[] expectedInt0 = { 1, null, null, null, 7 };
        long[] expectedRunningSum = { 1, 1, 1, 1, 8 };
        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            if (expectedInt0[i] == null) {
                assertNull("int0 at row " + i + " should be null", row.get(1));
            } else {
                assertEquals("int0 at row " + i, expectedInt0[i].intValue(), ((Number) row.get(1)).intValue());
            }
            assertEquals("running_sum at row " + i, expectedRunningSum[i], ((Number) row.get(2)).longValue());
        }
    }

    /**
     * Multi-shard correctness check for the same running-sum query as
     * {@link #testStreamstatsRunningSumOverInteger()}. The dataset is provisioned into a
     * {@value #MULTI_SHARD_COUNT}-shard index using the same documents.
     *
     * <p>Diagnostic: a global running window must execute over a totally-ordered stream.
     * If {@code RexOver(SUM ... OVER (ROWS UNBOUNDED PRECEDING))} were pushed to data nodes
     * and computed per-shard, each shard would emit its own independent running sum and the
     * coordinator would concatenate them — producing a result that is not a valid global
     * running sum. This test catches that latent bug; the single-shard test cannot.
     *
     * <p>Pinned values are identical to the single-shard test because the global running sum
     * over the same documents in the same key order is the same number, regardless of how
     * many shards the data is spread across. A failure here means the planner is not
     * coercing the windowed Project to a SINGLETON-distributed input.
     */
    public void testStreamstatsRunningSumOverInteger_multiShard() throws IOException {
        ensureProvisioned(DATASET_MULTI_SHARD, MULTI_SHARD_COUNT);
        Map<String, Object> response = executePplOnIndex(
            DATASET_MULTI_SHARD.indexName,
            "source=" + DATASET_MULTI_SHARD.indexName
                + " | sort key | streamstats sum(int0) as running_sum | fields key, int0, running_sum | head 5"
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected 5 rows from head 5", 5, rows.size());

        Integer[] expectedInt0 = { 1, null, null, null, 7 };
        long[] expectedRunningSum = { 1, 1, 1, 1, 8 };
        for (int i = 0; i < rows.size(); i++) {
            List<Object> row = rows.get(i);
            if (expectedInt0[i] == null) {
                assertNull("int0 at row " + i + " should be null", row.get(1));
            } else {
                assertEquals("int0 at row " + i, expectedInt0[i].intValue(), ((Number) row.get(1)).intValue());
            }
            assertEquals("running_sum at row " + i, expectedRunningSum[i], ((Number) row.get(2)).longValue());
        }
    }

    /**
     * Running count of rows. {@code count()} (no field arg) counts every row regardless of null
     * values, so the cumulative count is the row sequence number 1, 2, 3, ….
     */
    public void testStreamstatsRunningCountOverRows() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as running_count | fields key, running_count | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected 5 rows from head 5", 5, rows.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("running_count at row " + i, (long) (i + 1), ((Number) rows.get(i).get(1)).longValue());
        }
    }

    /**
     * Running min over int0. MIN ignores nulls. calcs sorted by key starts with
     * int0=[1, null, null, null, 7], so the cumulative min is [1, 1, 1, 1, 1].
     */
    public void testStreamstatsRunningMinOverInteger() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats min(int0) as running_min | fields key, int0, running_min | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(5, rows.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("running_min at row " + i, 1, ((Number) rows.get(i).get(2)).intValue());
        }
    }

    /**
     * Running max over int0. int0=[1, null, null, null, 7], so the cumulative max is
     * [1, 1, 1, 1, 7]. First-row-with-non-null-int0 then the new max once 7 arrives at row 4.
     */
    public void testStreamstatsRunningMaxOverInteger() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats max(int0) as running_max | fields key, int0, running_max | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(5, rows.size());
        int[] expectedMax = { 1, 1, 1, 1, 7 };
        for (int i = 0; i < 5; i++) {
            assertEquals("running_max at row " + i, expectedMax[i], ((Number) rows.get(i).get(2)).intValue());
        }
    }

    /**
     * Running avg over int0. AVG ignores nulls. calcs sorted by key starts with
     * int0=[1, null, null, null, 7] so the cumulative averages are [1, 1, 1, 1, (1+7)/2=4].
     *
     * <p>Currently blocked by the SQL plugin's AVG-OVER decomposition in
     * {@code PlanUtils.makeOver:178} which emits {@code Divide(Sum-OVER, Cast(Count-OVER))}.
     * The two nested Sum/Count window functions are buried inside arithmetic in the Project's
     * expression list — DataFusion's substrait consumer
     * ({@code datafusion-substrait/src/logical_plan/consumer/rel/project_rel.rs}) only
     * detects {@code Expr::WindowFunction} at the TOP LEVEL of each project expression and
     * silently leaves nested ones in place, so physical planning hits the catch-all in
     * {@code physical-expr/src/planner.rs::create_physical_expr}: "Physical plan does not
     * support logical expression WindowFunction".
     *
     * <p>SUM, MIN, MAX, COUNT pass through as a single top-level RexOver and work end-to-end.
     * AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP all hit the same nested-window bug due
     * to the same decomposition.
     *
     * <p>Fix is upstream in the SQL plugin — drop the AVG/STDDEV/VAR cases from
     * {@code PlanUtils.makeOver}, emit a single {@code RexOver} per function. Once landed and
     * the SNAPSHOT artifact picked up by our build, this test passes.
     */
    @AwaitsFix(bugUrl = "DataFusion Rust side does not implement Sum window function with frame 'Rows Preceding(NULL)..CurrentRow' (running sum). 'Physical plan does not support logical expression WindowFunction(... Sum)'. Runtime gap, not a planner regression.")
    public void testStreamstatsRunningAvgOverInteger() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats avg(int0) as running_avg | fields key, int0, running_avg | head 5"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(5, rows.size());
        double[] expectedAvg = { 1.0, 1.0, 1.0, 1.0, 4.0 };
        for (int i = 0; i < 5; i++) {
            assertEquals("running_avg at row " + i, expectedAvg[i], ((Number) rows.get(i).get(2)).doubleValue(), 0.0001);
        }
    }

    /**
     * Documents one upstream limitation exposed during the windowed-track expansion.
     *
     * <p><b>1. PPL's streamstats does not expose ranking / navigation window functions.</b>
     * The grammar (OpenSearchPPLParser.g4) lists ROW_NUMBER, RANK, DENSE_RANK, NTH, NTILE,
     * etc. under {@code scalarWindowFunctionName}, but {@code BuiltinFunctionName.WINDOW_FUNC_MAPPING}
     * (in core/.../BuiltinFunctionName.java) only maps aggregate-style names: {@code sum,
     * count, avg, min, max, var_pop, var_samp, std/stddev*, earliest, latest, distinct_count*,
     * pattern}. {@code CalciteRexNodeVisitor.visitWindowFunction} then throws
     * {@code UnsupportedOperationException("Unexpected window function: row_number")} for any
     * scalar window function. This blocks streamstats row_number / rank / dense_rank / nth at
     * the SQL-plugin layer; nothing in our analytics-engine can fix it.
     *
     */
    @SuppressWarnings("unused")
    public void testUpstreamLimitations_documentation_only() {
        // Intentionally empty. The javadoc documents what was tested and where it fails so the
        // next person doesn't go through the same investigation.
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        return executePplOnIndex(DATASET.indexName, ppl);
    }

    private Map<String, Object> executePplOnIndex(String indexName, String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
