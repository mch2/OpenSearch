/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link MvExpandIT} at two shards. That suite provisions a 1-shard index, so it cannot reach the
 * PARTIAL/FINAL reduce — and the explode is pushed down to the shard, which makes the reduce the
 * interesting part.
 *
 * <p>The specific risk is double expansion. {@code MultiValueRelRewriter} inserts the explode beneath
 * a group-by, and {@code OpenSearchAggregateSplitRule} splits the aggregate into a per-shard PARTIAL
 * and a coordinator FINAL. If the explode lands on both sides of that split, per-element counts
 * inflate — a failure that is invisible at one shard.
 *
 * <p>Same fixture as {@link MvExpandIT} under a distinct index name so the 1-shard index is untouched.
 * Totals are what matter here, not per-document rows, since shard routing is not fixed.
 */
public class MvExpandMultiShardIT extends AnalyticsRestTestCase {

    private static final String INDEX = "mvexpand_multishard_it";

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":2,\"number_of_replicas\":0}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"s1\",\"serviceName\":\"checkout\",\"durationMs\":120,"
                + "\"tags\":[\"prod\",\"us-east\"],\"codes\":[200,201]}\n"
                + "{\"index\":{}}\n{\"id\":\"s2\",\"serviceName\":\"payment\",\"durationMs\":450,"
                + "\"tags\":[\"prod\",\"us-west\"],\"codes\":[500,503]}\n"
                + "{\"index\":{}}\n{\"id\":\"s3\",\"serviceName\":\"cart\",\"durationMs\":30,"
                + "\"tags\":[\"staging\",\"us-west\",\"canary\"],\"codes\":[200]}\n"
                + "{\"index\":{}}\n{\"id\":\"s4\",\"serviceName\":\"checkout\",\"durationMs\":900,"
                + "\"tags\":[\"prod\"],\"codes\":[500]}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);
        provisioned = true;
    }

    /** Both shards' documents are expanded exactly once: 2 + 2 + 3 + 1 tag elements. */
    public void testExpandCoversEveryShardExactlyOnce() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags | stats count()", row(8));
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, codes | mvexpand codes | stats count()", row(6));
    }

    /**
     * The reduce case that motivates this suite: per-element buckets summed across shards. Inflated
     * counts here would mean the explode ran on both sides of the PARTIAL/FINAL split.
     */
    public void testPerElementGroupByReducesAcrossShards() throws IOException {
        List<List<Object>> expected = List.of(
            row(3, "prod"),      // s1, s2, s4
            row(2, "us-west"),   // s2, s3
            row(1, "us-east"),
            row(1, "staging"),
            row(1, "canary")
        );
        assertRowsEqualUnordered("source=" + INDEX + " | stats count() by tags", expected);
        assertRowsEqualUnordered("source=" + INDEX + " | fields tags | mvexpand tags | stats count() by tags", expected);
    }

    /** A scalar aggregate grouped by array elements — agg call and exploded key reduced together. */
    public void testAggregateScalarPerElementAcrossShards() throws IOException {
        assertRowsEqualUnordered(
            "source=" + INDEX + " | fields serviceName, durationMs, tags | mvexpand tags | stats avg(durationMs) by tags",
            row(490.0, "prod"),      // (120 + 450 + 900) / 3
            row(240.0, "us-west"),   // (450 + 30) / 2
            row(120.0, "us-east"),
            row(30.0, "staging"),
            row(30.0, "canary")
        );
    }

    /** {@code limit} is per document, so it must not be re-applied per shard or after the reduce. */
    public void testLimitIsPerDocumentAcrossShards() throws IOException {
        // s3 capped 3 -> 2; every other document is already at or below the cap. 2 + 2 + 2 + 1 = 7.
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags limit=2 | stats count()", row(7));
    }

    /** Filtering after the expansion, across shards — the shape that regressed at one shard. */
    public void testFilterOnExpandedFieldAcrossShards() throws IOException {
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, tags | mvexpand tags | where tags = 'prod' | stats count()", row(3));
        assertRowsEqualUnordered("source=" + INDEX + " | fields id, codes | mvexpand codes | where codes >= 500 | stats count()", row(3));
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqualUnordered(String ppl, List<Object>... expected) throws IOException {
        assertRowsEqualUnordered(ppl, Arrays.asList(expected));
    }

    private void assertRowsEqualUnordered(String ppl, List<List<Object>> expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actual = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actual);
        List<String> want = new ArrayList<>();
        for (List<Object> r : expected) {
            want.add(normalize(r));
        }
        List<String> got = new ArrayList<>();
        for (List<Object> r : actual) {
            got.add(normalize(r));
        }
        want.sort(null);
        got.sort(null);
        assertEquals("Rows mismatch for query: " + ppl, want, got);
    }

    /** Renders a row so numeric widths compare equal (a bigint may arrive as Integer or Long). */
    private static String normalize(List<Object> row) {
        StringBuilder sb = new StringBuilder();
        for (Object v : row) {
            sb.append('|');
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number n) {
                double d = n.doubleValue();
                sb.append(d == Math.rint(d) ? Long.toString((long) d) : Double.toString(d));
            } else {
                sb.append(v);
            }
        }
        return sb.toString();
    }
}
