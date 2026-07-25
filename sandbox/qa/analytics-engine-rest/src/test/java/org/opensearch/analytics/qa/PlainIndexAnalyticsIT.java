/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * End-to-end tests for analytics over a PLAIN index (spec: analytics-plain-index-spec.md): a
 * regular index — normal InternalEngine, normal DSL, normal merges, NO
 * {@code index.pluggable.dataformat} — opted into analytics scanning via
 * {@code index.analytics.scan.enabled}. The distributed engine scans the shard's ordinary Lucene
 * segments (the same BKD + doc values every index writes) through the doc-values leaf, via the
 * reader bridge in {@code IndexShard.getReaderProvider()}. Analytics is an additional reader, not a
 * different engine.
 *
 * <p>Differential baselines: (a) the SAME data in a composite lucene-primary index through the same
 * engine (the bridge-is-thin check — delta should be zero), and (b) classic {@code _search} DSL on
 * the plain index itself (the coexistence check).
 */
public class PlainIndexAnalyticsIT extends AnalyticsRestTestCase {

    private static final String PLAIN_INDEX = "plain_analytics_e2e";
    private static final String DV_INDEX = "plain_analytics_dv_baseline";
    private static final String FLAG = "analytics.query.distributed_engine";

    // ── Tests ──

    public void testGroupByMatchesCompositeDvBaseline() throws Exception {
        provisionBoth();
        assertPlainMatchesDv("| stats count() as n, sum(amount) as total by category | sort category");
    }

    public void testFullScanAggregate() throws Exception {
        provisionBoth();
        assertPlainMatchesDv("| stats count() as n, sum(amount) as total, avg(amount) as mean");
    }

    public void testResidualNumericFilter() throws Exception {
        provisionBoth();
        assertPlainMatchesDv("| where amount > 250 | stats count() as n, sum(amount) as total by category | sort category");
    }

    public void testDelegatedKeywordFilter() throws Exception {
        provisionBoth();
        assertPlainMatchesDv("| where category = 'b' | stats count() as n, sum(amount) as total");
    }

    public void testNullsAndDates() throws Exception {
        provisionBoth();
        assertPlainMatchesDv("| stats count(rating) as n, sum(rating) as total by category | sort category");
        assertPlainMatchesDv("| stats min(ts) as first, max(ts) as last by category | sort category");
    }

    /**
     * Coexistence (spec test group 2): the analytics opt-in must not change the index's engine or
     * its normal read surfaces. NOTE this QA cluster installs dsl-query-executor, which intercepts
     * {@code SearchAction} cluster-wide ({@code _search} AND {@code _count}, which routes through
     * it) — pre-existing harness behavior unrelated to the opt-in — so _search-level equivalence
     * cannot be asserted here. On a vanilla cluster coexistence holds by construction: analytics is
     * the only caller of {@code IndexShard.getReaderProvider()}, and the opt-in defaults to false
     * (covered by {@link #testOptOutIsNotScannable}). What this test asserts on THIS harness:
     * non-intercepted APIs ({@code _stats}, {@code _cat}, {@code _settings}) see a normal plain
     * index with the right doc counts, while analytics queries interleave successfully.
     */
    @SuppressWarnings("unchecked")
    public void testDslCoexistence() throws Exception {
        provisionBoth();
        setDistributedEngine(true);
        try {
            for (int round = 0; round < 3; round++) {
                // _stats (IndicesStatsAction — NOT intercepted): a plain engine reports real doc counts.
                Map<String, Object> stats = assertOkAndParse(
                    client().performRequest(new Request("GET", "/" + PLAIN_INDEX + "/_stats/docs")),
                    "_stats round " + round
                );
                Map<String, Object> primaries = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) stats.get("indices"))
                    .get(PLAIN_INDEX)).get("primaries");
                assertEquals(
                    "plain index must report real docs via _stats (a composite engine reports 0)",
                    120,
                    ((Number) ((Map<String, Object>) primaries.get("docs")).get("count")).intValue()
                );
                // _settings: no pluggable dataformat crept in; opt-in setting present.
                Map<String, Object> settings = assertOkAndParse(
                    client().performRequest(new Request("GET", "/" + PLAIN_INDEX + "/_settings?flat_settings=true")),
                    "_settings round " + round
                );
                Map<String, Object> idxSettings = (Map<String, Object>) ((Map<String, Object>) settings.get(PLAIN_INDEX)).get("settings");
                // The harness's cluster.pluggable.dataformat default injects the (inert) format KEY
                // into every index; what makes an index composite is the ENABLED flag. It must be off.
                assertNotEquals(
                    "index must stay plain (pluggable dataformat not enabled)",
                    "true",
                    String.valueOf(idxSettings.get("index.pluggable.dataformat.enabled"))
                );
                assertEquals("true", String.valueOf(idxSettings.get("index.analytics.scan.enabled")));
                // _cat works
                client().performRequest(new Request("GET", "/_cat/indices/" + PLAIN_INDEX));
                // analytics query between DSL calls — deterministic corpus: 4 categories, 30 docs each
                List<List<Object>> rows = rowsOf(
                    executePpl("source = " + PLAIN_INDEX + " | stats count() as n by category | sort category")
                );
                assertEquals(4, rows.size());
                for (List<Object> row : rows) {
                    assertEquals(30L, ((Number) row.get(0)).longValue());
                }
            }
        } finally {
            setDistributedEngine(false);
        }
    }

    /**
     * Analytics results equal ground truth computed from the deterministic corpus — the strongest
     * correctness check available without depending on this cluster's DSL-agg surface (the
     * dsl-query-executor intercepts _search cluster-wide and does not serve aggregations here).
     * Corpus: 120 docs, categories a-d round-robin (30 each), amount = (i*37)%500.
     */
    public void testAnalyticsMatchesGroundTruth() throws Exception {
        provisionBoth();
        // Compute expected sums per category from the generator.
        long[] sums = new long[4];
        for (int i = 0; i < 120; i++) {
            sums[i % 4] += (i * 37L) % 500;
        }
        setDistributedEngine(true);
        try {
            List<List<Object>> ppl = rowsOf(
                executePpl("source = " + PLAIN_INDEX + " | stats sum(amount) as t, count() as n by category | sort category")
            );
            String[] cats = { "a", "b", "c", "d" };
            assertEquals("bucket count", 4, ppl.size());
            for (int i = 0; i < 4; i++) {
                // PPL row: [t, n, category]
                List<Object> row = ppl.get(i);
                assertEquals("category " + i, cats[i], String.valueOf(row.get(2)));
                assertEquals("count for " + cats[i], 30L, ((Number) row.get(1)).longValue());
                assertEquals("sum for " + cats[i], sums[i], ((Number) row.get(0)).longValue());
            }
        } finally {
            setDistributedEngine(false);
        }
    }

    /**
     * Deletes (spec test group 3): analytics excludes deleted docs. Docs are indexed with explicit
     * ids and deleted via single-document DELETE (delete_by_query routes through the intercepted
     * _search path on this cluster, which can't serve it) — the Lucene-level effect (liveDocs) is
     * identical, which is what the scan-loop check must respect.
     */
    public void testDeletesRespected() throws Exception {
        String index = "plain_analytics_deletes";
        createPlainIndex(index, true);
        ingestWithIds(index);
        // Delete every doc in category 'd' (ids where i % 4 == 3) by id.
        for (int i = 3; i < 120; i += 4) {
            client().performRequest(new Request("DELETE", "/" + index + "/_doc/doc-" + i));
        }
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));

        setDistributedEngine(true);
        try {
            List<List<Object>> rows = rowsOf(executePpl("source = " + index + " | stats count() as n"));
            assertEquals("analytics count must exclude deletes", 90L, ((Number) rows.get(0).get(0)).longValue());
            List<List<Object>> byCat = rowsOf(executePpl("source = " + index + " | stats count() as n by category | sort category"));
            assertEquals("category d must be gone", 3, byCat.size());
            for (List<Object> row : byCat) {
                assertEquals(30L, ((Number) row.get(0)).longValue());
            }
        } finally {
            setDistributedEngine(false);
        }
    }

    /**
     * Lease hygiene (spec test group 4): repeated analytics queries then force-merge — a leaked
     * engine searcher would block the merge/close. Also NORMAL merges keep working (this is a plain
     * index; force-merge exercises the regular InternalEngine merge path, untouched by analytics).
     */
    public void testRepeatedQueriesReleaseSearchers() throws Exception {
        provisionBoth();
        setDistributedEngine(true);
        try {
            String query = "source = " + PLAIN_INDEX + " | stats count() as n, sum(amount) as total by category | sort category";
            List<List<Object>> first = rowsOf(executePpl(query));
            for (int i = 0; i < 20; i++) {
                assertEquals("iteration " + i, normalize(first), normalize(rowsOf(executePpl(query))));
            }
        } finally {
            setDistributedEngine(false);
        }
        Request merge = new Request("POST", "/" + PLAIN_INDEX + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        assertOkAndParse(client().performRequest(merge), "force-merge after repeated analytics queries");
        Map<String, Object> health = assertOkAndParse(
            client().performRequest(new Request("GET", "/_cluster/health/" + PLAIN_INDEX)),
            "health after repeated analytics queries"
        );
        assertNotEquals("index must not be RED", "red", health.get("status"));
        // And the index still answers analytics after the merge.
        setDistributedEngine(true);
        try {
            List<List<Object>> rows = rowsOf(executePpl("source = " + PLAIN_INDEX + " | stats count() as n"));
            assertEquals(120L, ((Number) rows.get(0).get(0)).longValue());
        } finally {
            setDistributedEngine(false);
        }
    }

    /** Opt-out: without index.analytics.scan.enabled the plain index must NOT be scannable. */
    public void testOptOutIsNotScannable() throws Exception {
        String index = "plain_analytics_no_optin";
        createPlainIndex(index, false);
        ingest(index);
        setDistributedEngine(true);
        try {
            Exception e = expectThrows(Exception.class, () -> executePpl("source = " + index + " | stats count() as n"));
            assertNotNull(e); // clear failure, not silent wrong answer (message shape varies by layer)
        } finally {
            setDistributedEngine(false);
        }
    }

    // ── Harness ──

    private void assertPlainMatchesDv(String pplTail) throws Exception {
        setDistributedEngine(true);
        try {
            List<List<Object>> dv = rowsOf(executePpl("source = " + DV_INDEX + " " + pplTail));
            List<List<Object>> plain = rowsOf(executePpl("source = " + PLAIN_INDEX + " " + pplTail));
            assertEquals("row count mismatch for: " + pplTail, dv.size(), plain.size());
            assertEquals("plain-index result must equal the composite-dv baseline for: " + pplTail, normalize(dv), normalize(plain));
        } finally {
            setDistributedEngine(false);
        }
    }

    private void setDistributedEngine(boolean on) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        // Reset to null (not false) when turning "off": a persistent false would override the
        // node-level default and poison every suite sharing this cluster (the plain-storage variant
        // runs with analytics.query.distributed_engine=true in opensearch.yml).
        req.setJsonEntity("{\"persistent\":{\"" + FLAG + "\": " + (on ? "true" : "null") + "}}");
        assertOkAndParse(client().performRequest(req), "set " + FLAG + "=" + on);
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rowsOf(Map<String, Object> result) {
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows must not be null", rows);
        return rows;
    }

    private static List<String> normalize(List<List<Object>> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (List<Object> r : rows) {
            out.add(r.toString());
        }
        return out;
    }

    // ── Provisioning ──

    private void provisionBoth() throws Exception {
        createPlainIndex(PLAIN_INDEX, true);
        createCompositeDvIndex(DV_INDEX);
        ingest(PLAIN_INDEX);
        ingest(DV_INDEX);
    }

    /** A completely NORMAL index — no pluggable dataformat — optionally opted into analytics scan. */
    private void createPlainIndex(String name, boolean analyticsOptIn) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String optIn = analyticsOptIn ? "\"index.analytics.scan.enabled\": true," : "";
        String body = String.format(Locale.ROOT, "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  %s"
            + "  \"index.number_of_routing_shards\": 2"
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"category\": { \"type\": \"keyword\" },"
            + "  \"amount\":   { \"type\": \"long\" },"
            + "  \"price\":    { \"type\": \"double\" },"
            + "  \"rating\":   { \"type\": \"integer\" },"
            + "  \"ts\":       { \"type\": \"date\" }"
            + "}}}", optIn);
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create " + name);
        waitGreen(name);
    }

    /** The composite lucene-primary baseline (the existing dv-leaf path). */
    private void createCompositeDvIndex(String name) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"lucene\","
            + "  \"index.number_of_routing_shards\": 2"
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"category\": { \"type\": \"keyword\" },"
            + "  \"amount\":   { \"type\": \"long\" },"
            + "  \"price\":    { \"type\": \"double\" },"
            + "  \"rating\":   { \"type\": \"integer\" },"
            + "  \"ts\":       { \"type\": \"date\" }"
            + "}}}";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create " + name);
        waitGreen(name);
    }

    private void waitGreen(String name) throws Exception {
        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /** As {@link #ingest} but with explicit doc ids ({@code doc-<i>}) so tests can DELETE by id. */
    private void ingestWithIds(String index) throws Exception {
        String[] categories = { "a", "b", "c", "d" };
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            String cat = categories[i % categories.length];
            long amount = (i * 37L) % 500;
            bulk.append("{\"index\":{\"_id\":\"doc-").append(i).append("\"}}\n");
            bulk.append("{\"category\":\"").append(cat).append("\",\"amount\":").append(amount).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + index);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
    }

    /** Same deterministic corpus as DocValuesLeafIT: 120 docs, categories a-d, nulls every 3rd. */
    private void ingest(String index) throws Exception {
        String[] categories = { "a", "b", "c", "d" };
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            String cat = categories[i % categories.length];
            long amount = (i * 37L) % 500;
            double price = (i % 50) + 0.25;
            String rating = (i % 3 == 0) ? null : String.valueOf(i % 10);
            long ts = 1700000000000L + i * 60_000L;
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"category\":\"").append(cat).append("\",\"amount\":").append(amount).append(",\"price\":").append(price);
            if (rating != null) {
                bulk.append(",\"rating\":").append(rating);
            }
            bulk.append(",\"ts\":").append(ts).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + index);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }
}
