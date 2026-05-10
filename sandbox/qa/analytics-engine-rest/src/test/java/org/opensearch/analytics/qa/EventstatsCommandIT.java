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
 * End-to-end integration test for PPL {@code eventstats} on the analytics-engine route.
 *
 * <p>{@code eventstats} lowers to {@code <agg>(field) OVER ()} — an unbounded window over
 * the entire input that broadcasts a global aggregate value to every row. Lower-form is the
 * same shape as {@code streamstats} (a {@link org.apache.calcite.rex.RexOver} inside an
 * {@link org.apache.calcite.rel.core.Project}) just with a different frame: {@code OVER ()}
 * vs {@code OVER (ROWS UNBOUNDED PRECEDING)}. {@code OpenSearchWindowedProjectGatherRule}
 * treats both identically, so anything that works for streamstats's running aggregate
 * works for eventstats's broadcast aggregate too.
 *
 * <p>Each test pins the broadcast value across multiple rows. If the windowed gather were
 * mis-positioned (e.g. computed per-shard and concatenated), the broadcast would vary by
 * shard and the cross-row equality check would fail.
 */
public class EventstatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static final Set<String> PROVISIONED = new HashSet<>();

    private void ensureDataProvisioned() throws IOException {
        if (PROVISIONED.add(DATASET.indexName)) {
            DatasetProvisioner.provision(client(), DATASET);
        }
    }

    /**
     * {@code count()} broadcasts the total row count. calcs has 17 rows; every row in the
     * result gets {@code global_count = 17}. {@code head 3} returns the first three rows; all
     * three should report the same global value.
     */
    public void testEventstatsCountBroadcastsGlobalRowCount() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats count() as global_count | fields key, global_count | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected 3 rows from head 3", 3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("global_count at row " + i, 17L, ((Number) rows.get(i).get(1)).longValue());
        }
    }

    /**
     * {@code max(int0)} broadcasts the global maximum. int0 non-null values across all 17 rows
     * are 1, 3, 4, 4, 4, 7, 8, 8, 8, 10, 11 — max = 11. Every row should report {@code global_max = 11}.
     */
    public void testEventstatsMaxBroadcastsGlobalMax() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats max(int0) as global_max | fields key, int0, global_max | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("global_max at row " + i, 11, ((Number) rows.get(i).get(2)).intValue());
        }
    }

    /**
     * {@code sum(int0)} broadcasts the global sum. Sum of int0 non-null values = 1+3+4+4+4+7+8+8+8+10+11 = 68.
     */
    public void testEventstatsSumBroadcastsGlobalSum() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats sum(int0) as global_sum | fields key, int0, global_sum | head 3"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull(rows);
        assertEquals(3, rows.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("global_sum at row " + i, 68L, ((Number) rows.get(i).get(2)).longValue());
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
