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
