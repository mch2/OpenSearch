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
 * End-to-end tests for the doc-values-backed distributed leaf (PoC 1): a LUCENE-PRIMARY composite
 * index (no parquet anywhere in the read path), scanned through the distributed engine's
 * {@code openFragment} JAVA_CURSOR mode — Lucene selects doc IDs, Java decodes doc values into
 * Arrow batches, DataFusion pulls them and does all compute (partial agg → shuffle → final agg).
 *
 * <p>Differential baseline: the SAME data in a parquet-primary index queried through the SAME
 * distributed engine. The legacy (non-distributed) engine cannot scan a lucene-primary index at
 * all, so parquet-vs-docvalues under one engine is the honest comparison — it isolates the storage
 * term exactly as the PoC benchmark plan prescribes.
 */
public class DocValuesLeafIT extends AnalyticsRestTestCase {

    private static final String DV_INDEX = "dv_leaf_e2e";
    private static final String PARQUET_INDEX = "dv_leaf_parquet_baseline";
    private static final String FLAG = "analytics.query.distributed_engine";

    // ── Tests ──

    /** Build-order step 1 target: distributed `stats count() by long-ish group` over 2 shards. */
    public void testGroupByLongMatchesParquetBaseline() throws Exception {
        provisionBoth();
        assertDvMatchesParquet("| stats count() as n, sum(amount) as total by category | sort category");
    }

    public void testFullScanAggregateNoPredicate() throws Exception {
        provisionBoth();
        assertDvMatchesParquet("| stats count() as n, sum(amount) as total, avg(amount) as mean");
    }

    public void testResidualNumericFilterRunsInDataFusion() throws Exception {
        provisionBoth();
        // Numeric predicates don't delegate to Lucene (Index caps cover keyword/text only) — this
        // exercises MatchAll scan + residual FilterExec above the DV leaf.
        assertDvMatchesParquet("| where amount > 250 | stats count() as n, sum(amount) as total by category | sort category");
    }

    public void testDelegatedKeywordFilter() throws Exception {
        provisionBoth();
        // Keyword equality delegates to Lucene → the DV leaf runs a real TermQuery, not MatchAll.
        assertDvMatchesParquet("| where category = 'b' | stats count() as n, sum(amount) as total");
    }

    public void testMinMaxAndDoubles() throws Exception {
        provisionBoth();
        assertDvMatchesParquet("| stats min(amount) as lo, max(amount) as hi, sum(price) as p by category | sort category");
    }

    public void testNullBearingColumn() throws Exception {
        provisionBoth();
        // `rating` is null on every third doc — nulls must agree with the parquet path.
        assertDvMatchesParquet("| stats count(rating) as n, sum(rating) as total by category | sort category");
    }

    public void testDateColumn() throws Exception {
        provisionBoth();
        assertDvMatchesParquet("| stats min(ts) as first, max(ts) as last by category | sort category");
    }

    public void testExactCountDistinct() throws Exception {
        provisionBoth();
        assertDvMatchesParquet("| stats distinct_count(category) as dc");
    }

    /**
     * The rest of the differential corpus (spec: >=15 queries): delegated+residual mixes, keyword
     * group-bys (utf8 mode), IS NULL semantics, ranges, avg over ints, multi-key grouping. Batched
     * in one test to keep cluster spin-up cost sane; each query asserts independently.
     */
    public void testDifferentialCorpus() throws Exception {
        provisionBoth();
        String[] corpus = {
            // delegated keyword + residual numeric in one WHERE
            "| where category = 'a' and amount > 100 | stats count() as n, sum(amount) as s",
            // keyword group-by with keyword filter (delegated) — group keys decode via utf8
            "| where category != 'd' | stats count() as n by category | sort category",
            // is-null / is-not-null semantics must agree with the parquet path
            "| where isnull(rating) | stats count() as n by category | sort category",
            "| where isnotnull(rating) | stats count() as n, sum(rating) as s by category | sort category",
            // numeric range (residual) + avg over int column
            "| where amount >= 100 and amount < 400 | stats avg(rating) as m by category | sort category",
            // double column arithmetic
            "| stats sum(price) as p, avg(price) as ap",
            // date range residual filter
            "| where ts > '2023-11-14 22:30:00' | stats count() as n",
            // multi-key group-by (keyword + int bucket)
            "| stats count() as n by category, rating | sort category, rating",
            // count over the keyword column itself
            "| stats count(category) as n",
            // min/max over dates grouped by keyword
            "| stats min(ts) as lo, max(ts) as hi by category | sort category" };
        for (String tail : corpus) {
            assertDvMatchesParquet(tail);
        }
    }

    /**
     * v1 exclusion (spec J3): an unsupported mapping type (`ip`) must fail CLEARLY — never a silent
     * wrong answer. On a lucene-primary index the exclusion fires at the earliest possible point:
     * the composite capability assignment rejects INDEX CREATION because the lucene format claims
     * no ip capabilities, so a bad column can never even reach the scan. (The DV leaf's own typed
     * DocValuesLeafUnsupportedException remains the backstop for types that map but don't decode.)
     */
    public void testUnsupportedTypeFailsClearly() throws Exception {
        String index = "dv_leaf_ip_excl";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1, \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"lucene\""
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"addr\": { \"type\": \"ip\" },"
            + "  \"amount\": { \"type\": \"long\" }"
            + "}}}";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        var e = expectThrows(org.opensearch.client.ResponseException.class, () -> client().performRequest(create));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertTrue(
            "rejection must name the unsupported field: " + e.getMessage(),
            e.getMessage().contains("addr") && e.getMessage().contains("ip")
        );
    }

    /** Repeated queries must not leak reader leases; force-merge requires all readers released. */
    public void testRepeatedQueriesReleaseLeases() throws Exception {
        provisionBoth();
        setDistributedEngine(true);
        try {
            String query = "source = " + DV_INDEX + " | stats count() as n, sum(amount) as total by category | sort category";
            List<List<Object>> first = rowsOf(executePpl(query));
            for (int i = 0; i < 20; i++) {
                assertEquals("iteration " + i, normalize(first), normalize(rowsOf(executePpl(query))));
            }
        } finally {
            setDistributedEngine(false);
        }
        Request merge = new Request("POST", "/" + DV_INDEX + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        assertOkAndParse(client().performRequest(merge), "force-merge after repeated dv queries");
        Map<String, Object> health = assertOkAndParse(
            client().performRequest(new Request("GET", "/_cluster/health/" + DV_INDEX)),
            "health after repeated dv queries"
        );
        assertNotEquals("index must not be RED", "red", health.get("status"));
    }

    // ── Harness ──

    /** Runs {@code pplTail} against both indices under the distributed engine; results must match. */
    private void assertDvMatchesParquet(String pplTail) throws Exception {
        setDistributedEngine(true);
        try {
            List<List<Object>> parquet = rowsOf(executePpl("source = " + PARQUET_INDEX + " " + pplTail));
            List<List<Object>> docValues = rowsOf(executePpl("source = " + DV_INDEX + " " + pplTail));
            assertEquals("row count mismatch for: " + pplTail, parquet.size(), docValues.size());
            assertEquals("doc-values result must equal the parquet baseline for: " + pplTail, normalize(parquet), normalize(docValues));
        } finally {
            setDistributedEngine(false);
        }
    }

    private void setDistributedEngine(boolean on) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"" + FLAG + "\": " + on + "}}");
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
        createIndex(DV_INDEX, "lucene", 2);
        createIndex(PARQUET_INDEX, "parquet", 2);
        ingest(DV_INDEX);
        ingest(PARQUET_INDEX);
    }

    /**
     * Composite index with the given PRIMARY data format. For "lucene" primary there is no parquet:
     * doc values live in the Lucene segments and the DV leaf is the only scan path. For "parquet"
     * primary, lucene rides as secondary (the standard analytics layout) — the baseline.
     */
    private void createIndex(String name, String primaryFormat, int shards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}
        String secondary = primaryFormat.equals("parquet") ? "\"index.composite.secondary_data_formats\": [\"lucene\"]," : "";
        String body = String.format(
            Locale.ROOT,
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": %d,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"%s\","
                + "  %s"
                + "  \"index.number_of_routing_shards\": %d"
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"category\": { \"type\": \"keyword\" },"
                + "    \"amount\":   { \"type\": \"long\" },"
                + "    \"price\":    { \"type\": \"double\" },"
                + "    \"rating\":   { \"type\": \"integer\" },"
                + "    \"ts\":       { \"type\": \"date\" }"
                + "  }"
                + "}"
                + "}",
            shards,
            primaryFormat,
            secondary,
            shards
        );
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + name);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));
        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * 120 deterministic docs over categories a/b/c/d: amounts/prices derived from i, `rating` null
     * on every third doc (null-decode coverage), timestamps spread over one day. Identical bodies
     * go to both indices so the differential is exact.
     */
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
