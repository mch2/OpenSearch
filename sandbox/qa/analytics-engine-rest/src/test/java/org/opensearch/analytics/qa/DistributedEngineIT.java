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
import java.util.Map;

/**
 * End-to-end test for the {@code datafusion-distributed} execution path (Model B).
 *
 * <p>Creates a 2-shard parquet-backed index, then runs the SAME {@code GROUP BY} aggregation twice:
 * once with {@code analytics.query.distributed_engine=false} (the legacy Java DAG/scheduler path) and
 * once with it {@code true} (the new path: whole-query Substrait → Rust coordinator → df-distributed
 * staging → data-node Rust Worker leaf that upcalls Java for the unchanged shard scan → rust↔rust
 * shuffle → coordinator reduce). The two result sets must be identical — proving the distributed
 * engine returns correct answers across shards.
 */
public class DistributedEngineIT extends AnalyticsRestTestCase {

    private static final String INDEX = "distributed_engine_e2e";
    private static final String INDEX2 = "distributed_engine_e2e_2";
    private static final String TEXT_INDEX = "distributed_engine_text_e2e";
    private static final String FLAG = "analytics.query.distributed_engine";

    /** Runs {@code query} with the flag off (legacy) then on (distributed), asserting equal results. */
    private void assertDistributedMatchesLegacy(String query) throws Exception {
        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));
        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }
        assertEquals("row count mismatch for: " + query, legacy.size(), distributed.size());
        assertEquals("result mismatch for: " + query, normalize(legacy), normalize(distributed));
    }

    public void testDistributedGroupByMatchesLegacy() throws Exception {
        createParquetBackedIndex();
        ingestDeterministicDocs();

        String query = "source = " + INDEX + " | stats count() as n, sum(amount) as total by category | sort category";

        // Baseline: legacy Java distributed path (flag off).
        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));

        // New path: datafusion-distributed (flag on).
        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }

        assertEquals("distributed engine must return the same number of groups as legacy", legacy.size(), distributed.size());
        assertEquals(
            "distributed engine GROUP BY result must equal the legacy path",
            normalize(legacy),
            normalize(distributed)
        );
        // Sanity: 3 categories (a,b,c) from the deterministic dataset.
        assertEquals("expected 3 category groups", 3, distributed.size());
    }

    /**
     * Probes a spread of query shapes against the distributed engine, asserting each matches the
     * legacy path. This is the breadth check: aggregates (with/without group key, avg, multi-key),
     * a global aggregate, a filtered aggregate, and non-aggregate projection/filter/limit shapes.
     */
    public void testDistributedShapesMatchLegacy() throws Exception {
        createParquetBackedIndex();
        ingestDeterministicDocs();

        String[] queries = new String[] {
            // min/max BY key — a second two-phase group-by shape (non-sum accumulators)
            "source = " + INDEX + " | stats min(amount) as lo, max(amount) as hi by category | sort category",
            // filter then aggregate BY key — exercises annotation-stripping (WHERE predicate resolved
            // to a native filter via convertWholeQuery; parquet evaluates it, no Lucene delegation).
            "source = " + INDEX + " | where amount > 20 | stats count() as n by category | sort category",
            // global aggregate (NO GROUP BY) — single-partition final, no shuffle key.
            "source = " + INDEX + " | stats count() as n, sum(amount) as total",
        };

        for (String query : queries) {
            setDistributedEngine(false);
            List<List<Object>> legacy = rowsOf(executePpl(query));
            setDistributedEngine(true);
            List<List<Object>> distributed;
            try {
                distributed = rowsOf(executePpl(query));
            } finally {
                setDistributedEngine(false);
            }
            assertEquals("row count mismatch for query: " + query, legacy.size(), distributed.size());
            assertEquals("result mismatch for query: " + query, normalize(legacy), normalize(distributed));
        }
    }

    /**
     * Higher-cardinality GROUP BY to exercise a genuine HASH shuffle: with many distinct keys spread
     * across both shards, the Partial aggregate on each shard emits partial groups that must be
     * hash-repartitioned on the group key (NetworkShuffleExec) so every key's partials converge on one
     * FinalPartitioned partition. The result must still match the legacy path exactly. The distributed
     * physical plan shape (AggregateExec(Partial) → NetworkShuffleExec → AggregateExec(FinalPartitioned))
     * is logged at INFO by the coordinator ("distributed physical plan:").
     */
    public void testDistributedHashShuffleGroupBy() throws Exception {
        createParquetBackedIndex();
        ingestHashShuffleDocs();

        String query = "source = " + INDEX + " | stats count() as n, sum(amount) as total by category | sort category";

        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));
        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }
        assertEquals("hash-shuffle group count mismatch", legacy.size(), distributed.size());
        assertEquals("hash-shuffle GROUP BY result must equal legacy", normalize(legacy), normalize(distributed));
        // 20 distinct keys k00..k19 → 20 groups, forcing real cross-shard hash repartition.
        assertEquals("expected 20 category groups", 20, distributed.size());
    }

    /**
     * HIGH-CARDINALITY group-by: 500 docs across 500 DISTINCT keys spread over both shards. With
     * partial_reduce enabled (coordinator default), each shard's Partial aggregate is merged locally
     * ABOVE the hash repartition (PartialReduce) BEFORE the network shuffle — so the shuffle carries
     * one row per (key,shard) rather than every raw row, the mechanism that keeps a wide group-by from
     * bottlenecking the shuffle + coordinator. Result must still equal the legacy path exactly.
     */
    public void testDistributedHighCardinalityGroupBy() throws Exception {
        createParquetBackedIndex();
        ingestHighCardinalityDocs();

        String query = "source = " + INDEX + " | stats count() as n, sum(amount) as total by category | sort category";

        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));
        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }
        assertEquals("high-cardinality group count mismatch", legacy.size(), distributed.size());
        assertEquals("high-cardinality GROUP BY result must equal legacy", normalize(legacy), normalize(distributed));
        assertEquals("expected 500 distinct groups", 500, distributed.size());
    }

    /**
     * Multi-table JOIN on the distributed path (task b): {@code register_shard_tables} registers a
     * {@code ShardScanTable} for EVERY distinct NamedTable leaf, so a self-join binds both legs. The
     * distributed result must equal the legacy path.
     */
    public void testDistributedSelfJoinMatchesLegacy() throws Exception {
        createParquetBackedIndex();
        ingestDeterministicDocs();

        // Self-join on category, then aggregate.
        String query = "source = " + INDEX + " | join left=l right=r on l.category = r.category " + INDEX
            + " | stats count() as n";

        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));

        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }
        assertEquals("distributed self-join row count must equal legacy", legacy.size(), distributed.size());
        assertEquals("distributed self-join result must equal legacy", normalize(legacy), normalize(distributed));

        // Node must still be healthy (no native crash, no dropped node).
        Map<String, Object> health = assertOkAndParse(
            client().performRequest(new Request("GET", "/_cluster/health")),
            "cluster health after distributed join"
        );
        assertNotEquals("cluster must not be RED after a distributed join", "red", health.get("status"));
    }

    /**
     * All join shapes across TWO indices with DIFFERENT shard counts (INDEX=2 shards, INDEX2=3), so
     * per-table shard routing is exercised. Covers inner/left/right joins, join+filter, and
     * join+group-by. Each distributed result must equal the legacy path.
     */
    public void testDistributedCrossIndexJoins() throws Exception {
        createParquetBackedIndex();
        ingestDeterministicDocs();
        createSecondParquetIndex();

        // inner join + aggregate
        assertDistributedMatchesLegacy(
            "source = " + INDEX + " | join left=l right=r on l.category = r.category " + INDEX2 + " | stats count() as n"
        );
        // inner join + group-by on the join key
        assertDistributedMatchesLegacy(
            "source = " + INDEX + " | join left=l right=r on l.category = r.category " + INDEX2
                + " | stats count() as n by l.category | sort `l.category`"
        );
        // left outer join (categories in INDEX with no INDEX2 match still appear)
        assertDistributedMatchesLegacy(
            "source = " + INDEX + " | left join left=l right=r on l.category = r.category " + INDEX2 + " | stats count() as n"
        );
        // join then filter
        assertDistributedMatchesLegacy(
            "source = " + INDEX + " | join left=l right=r on l.category = r.category " + INDEX2
                + " | where l.amount > 20 | stats count() as n"
        );
    }

    /**
     * INDEXED query (case 2): a {@code match()} predicate that delegates to the Lucene secondary.
     *
     * <p>Full distributed indexed path: {@code convertWholeQuery} resolves the marked tree to the
     * datafusion driver (PlanForker.resolveWholeQuery), so the Lucene-only {@code match} predicate is
     * turned into a {@code delegated_predicate(id)} marker + its serialized Lucene query is captured
     * into the {@code DelegationDescriptor}. {@code ShardScanTable} pushes the marker into
     * {@code ShardScanExec} (dropping the FilterExec); the codec ships the descriptor + leaf fragment;
     * the leaf upcall registers the {@code FilterDelegationHandle} and runs the indexed (Lucene)
     * executor. Result must equal the legacy path.
     */
    public void testDistributedIndexedMatchMatchesLegacy() throws Exception {
        createTextIndex();
        ingestTextDocs();

        String query = "source = " + TEXT_INDEX + " | where match(body, 'error') | stats count() as n by service | sort service";

        setDistributedEngine(false);
        List<List<Object>> legacy = rowsOf(executePpl(query));
        setDistributedEngine(true);
        List<List<Object>> distributed;
        try {
            distributed = rowsOf(executePpl(query));
        } finally {
            setDistributedEngine(false);
        }
        assertEquals("indexed match() group count mismatch", legacy.size(), distributed.size());
        assertEquals("indexed match() result must equal legacy", normalize(legacy), normalize(distributed));
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

    /** Stringify each row so numeric long/int representation differences don't cause false negatives. */
    private static List<String> normalize(List<List<Object>> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (List<Object> r : rows) {
            out.add(r.toString());
        }
        return out;
    }

    /**
     * 200 docs across 20 distinct keys (k00..k19), interleaved so each key lands on both shards by
     * _id hash — the Partial aggregate cannot pre-combine a key fully on one shard, so the hash
     * shuffle genuinely moves partials. amount = deterministic per (key,i) so totals are reproducible.
     */
    private void ingestHashShuffleDocs() throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            int k = i % 20;
            String cat = String.format("k%02d", k);
            int amount = k * 100 + i; // distinct, deterministic
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"category\":\"").append(cat).append("\",\"amount\":").append(amount).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    /** 500 docs, each a DISTINCT category (hc000..hc499) → 500 groups, spread across both shards. */
    private void ingestHighCardinalityDocs() throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            String cat = String.format("hc%03d", i);
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"category\":\"").append(cat).append("\",\"amount\":").append(i).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    private void createParquetBackedIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"category\": { \"type\": \"keyword\" },"
            + "    \"amount\":   { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}";
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * A SECOND parquet-backed index with a DIFFERENT shard count (3 vs INDEX's 2) so cross-index joins
     * exercise per-table shard routing (the two join legs have different shard layouts). Same schema
     * (category, amount) so it can join INDEX on category.
     */
    private void createSecondParquetIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX2));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 3,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"category\": { \"type\": \"keyword\" },"
            + "    \"weight\":   { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}";
        Request create = new Request("PUT", "/" + INDEX2);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX2);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX2);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Categories a/b/d (d has no match in INDEX; c in INDEX has no match here — exercises inner vs outer).
        String[] categories = { "a", "b", "a", "d", "b", "d" };
        int[] weights = { 1, 2, 3, 4, 5, 6 };
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < categories.length; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"category\":\"").append(categories[i]).append("\",\"weight\":").append(weights[i]).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX2 + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX2);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX2 + "/_flush?force=true"));
    }

    private void ingestDeterministicDocs() throws Exception {
        // 6 docs across categories a/b/c so a GROUP BY is non-trivial and spans both shards (by _id
        // hash). Totals: a = 10+30+60 = 100 (n=3); b = 20+50 = 70 (n=2); c = 40 (n=1).
        int[] amounts = { 10, 20, 30, 40, 50, 60 };
        String[] categories = { "a", "b", "a", "c", "b", "a" };
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < amounts.length; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"category\":\"").append(categories[i]).append("\",\"amount\":").append(amounts[i]).append("}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    /** 2-shard composite index with a text field so {@code match()} delegates to the Lucene secondary. */
    private void createTextIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + TEXT_INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"service\": { \"type\": \"keyword\" },"
            // match_only_text lives ONLY in the Lucene secondary (no parquet writer), so a match()
            // predicate on it is Lucene-only → CORRECTNESS delegation: the whole call is replaced with
            // a delegated_predicate(id) marker (no retained options MAP that isthmus can't serialize).
            + "    \"body\":    { \"type\": \"match_only_text\" }"
            + "  }"
            + "}"
            + "}";
        Request create = new Request("PUT", "/" + TEXT_INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + TEXT_INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + TEXT_INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void ingestTextDocs() throws Exception {
        // 8 docs; some contain the token "error" in body, spread across 3 services and both shards.
        String[] services = { "checkout", "search", "checkout", "payments", "search", "checkout", "payments", "search" };
        String[] bodies = {
            "request failed with error",
            "all good here",
            "fatal error during commit",
            "error talking to upstream",
            "cache warm ok",
            "timeout then error retry",
            "healthy",
            "error error everywhere" };
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < services.length; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"service\":\"").append(services[i]).append("\",\"body\":\"").append(bodies[i]).append("\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + TEXT_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + TEXT_INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + TEXT_INDEX + "/_flush?force=true"));
    }
}
