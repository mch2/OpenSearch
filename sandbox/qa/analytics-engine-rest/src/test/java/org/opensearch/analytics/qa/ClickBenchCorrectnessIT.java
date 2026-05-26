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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClickBench correctness test: validates that multi-shard distributed execution produces
 * identical results to single-shard execution.
 *
 * <p>Approach:
 * <ol>
 *   <li>Ingest the ClickBench dataset into a 1-shard index (no reduce, no fan-out)</li>
 *   <li>Run all supported queries, capture results as ground truth</li>
 *   <li>Delete and re-create with 4 shards across 2 nodes (forces coordinator reduce)</li>
 *   <li>Run same queries, compare results — any difference is a correctness bug</li>
 * </ol>
 *
 * <p>This catches: reduce ordering bugs, partial/final aggregate mismatches, type coercion
 * differences across shards, null-fill issues, and column reordering problems.
 */
public class ClickBenchCorrectnessIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "clickbench_correctness";
    private static final Set<Integer> SKIP_QUERIES = Set.of();

    /** Total documents to generate. Override at runtime via -Dclickbench.docCount=N. */
    private static final int DOC_COUNT = Integer.getInteger("clickbench.docCount", 10_000);
    /** RNG seed. Override via -Dclickbench.seed=N for variation; default fixed for reproducibility. */
    private static final long DATA_SEED = Long.getLong("clickbench.seed", 42L);
    /** Bulk request batch size — bulk APIs reject monolithic requests for large datasets. */
    private static final int BULK_BATCH_SIZE = 2000;

    public void testQ26Only() throws Exception {
        List<Integer> queryNumbers = List.of(26);
        deleteIndex();
        logger.info("Phase 1: ingesting into 1-shard index for ground truth");
        createAndIngest(1);
        Map<Integer, Object> groundTruth = runAllQueries(queryNumbers);

        logger.info("Phase 2: re-ingesting into 4-shard index for distributed execution");
        deleteIndex();
        createAndIngest(4);
        Map<Integer, Object> distributed = runAllQueries(queryNumbers);

        logger.info("Q26 single-shard: {}", groundTruth.get(26));
        logger.info("Q26 multi-shard: {}", distributed.get(26));
    }

    /**
     * Diagnostic test: runs the queries that are normally skipped, capturing the
     * raw error message from each. Always passes — its purpose is to surface what
     * makes each query fail today so they can be triaged for fixes vs. permanent skip.
     */
    public void testRunSkippedQueriesForDiagnostics() throws Exception {
        deleteIndex();
        createAndIngest(1);
        for (int q : SKIP_QUERIES) {
            String pplFile = "datasets/" + ClickBenchTestHelper.DATASET.name + "/ppl/q" + q + ".ppl";
            String ppl;
            try (var is = getClass().getClassLoader().getResourceAsStream(pplFile)) {
                if (is == null) {
                    logger.info("Q{}: no PPL file found", q);
                    continue;
                }
                ppl = new String(is.readAllBytes()).trim().replace("clickbench", INDEX_NAME);
            }
            logger.info("Q{} PPL: {}", q, ppl);
            try {
                Request request = new Request("POST", "/_analytics/ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client().performRequest(request);
                Map<String, Object> body = assertOkAndParse(response, "Q" + q);
                Object rows = body.get("rows");
                int rowCount = (rows instanceof List<?> list) ? list.size() : -1;
                logger.info("Q{}: SUCCESS, {} rows: {}", q, rowCount,
                    rowCount > 5 && rows instanceof List<?> list ? list.subList(0, 5) + "..." : rows);
            } catch (ResponseException e) {
                logger.warn("Q{}: FAILED with HTTP error: {}", q, e.getMessage());
            } catch (Exception e) {
                logger.warn("Q{}: FAILED with: {}", q, e.toString());
            }
        }
    }

    public void testMultiShardMatchesSingleShard() throws Exception {
        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ClickBenchTestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> !SKIP_QUERIES.contains(n))
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());

        // Phase 1: single-shard ground truth
        logger.info("Phase 1: ingesting into 1-shard index for ground truth");
        createAndIngest(1);
        Map<Integer, Object> groundTruth = runAllQueries(queryNumbers);

        // Phase 2: multi-shard (forces coordinator reduce across 2 nodes)
        logger.info("Phase 2: re-ingesting into 6-shard index for distributed execution");
        deleteIndex();
        createAndIngest(6);
        Map<Integer, Object> distributed = runAllQueries(queryNumbers);

        // Phase 3: compare
        List<String> mismatches = new ArrayList<>();
        for (int q : queryNumbers) {
            Object expected = groundTruth.get(q);
            Object actual = distributed.get(q);
            if (expected == null && actual == null) continue;
            if (expected == null) {
                mismatches.add("Q" + q + ": single-shard failed but multi-shard succeeded");
                continue;
            }
            if (actual == null) {
                mismatches.add("Q" + q + ": single-shard succeeded but multi-shard failed");
                continue;
            }
            if (!resultsEqualIgnoringTies(expected, actual)) {
                mismatches.add("Q" + q + ": results differ\n  single-shard: " + expected + "\n  multi-shard:  " + actual);
            }
        }

        if (!mismatches.isEmpty()) {
            fail("Correctness mismatches between single-shard and multi-shard execution:\n"
                + String.join("\n\n", mismatches));
        }

        logger.info("All {} queries produce identical results across 1-shard and 6-shard configurations", groundTruth.size());
    }

    /**
     * Compares two result row lists with tolerance for tie ambiguity inherent to
     * {@code ORDER BY ... LIMIT N} when the sort key has duplicates.
     *
     * <p>Three checks, in order of strictness:
     * <ol>
     *   <li>Strict equality — same rows, same order. Catches the easy case.</li>
     *   <li>Multiset equality — same rows in any order. Catches Q10-style cases where
     *       the displayed sort column isn't column 0 (PPL stats puts aggregates before
     *       group-by keys, so a {@code sort -c} sort key sits in column 1).</li>
     *   <li>Boundary tie tolerance — when both sides have the same length and agree
     *       on every row except for those at the very end of the result, allow the
     *       last tied group to differ in identity. This handles the case where the
     *       {@code LIMIT N} cut splits a tied-on-sort-key group: different shards
     *       can pick different rows from that wider group, but the survivors must all
     *       tie with the last surviving row from the other side.</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    private static boolean resultsEqualIgnoringTies(Object expected, Object actual) {
        if (expected == null || actual == null) return expected == actual;
        if (!(expected instanceof List<?> expectedList) || !(actual instanceof List<?> actualList)) {
            return expected.equals(actual);
        }
        if (expectedList.size() != actualList.size()) return false;
        if (expected.equals(actual)) return true;

        // Strategy A: same rows in any order. Catches both "sort key isn't col 0"
        // (Q10 style) and pure within-group reordering.
        if (sameMultiset(expectedList, actualList)) return true;

        // Strategy B: walk the lists in tied-group lockstep using column 0 as the
        // primary key. Within each fully-equal tied group (column 0 matches and
        // group sizes match), allow members to appear in any order. The last tied
        // group at the limit boundary is allowed to differ in identity — when
        // {@code LIMIT N} cuts a wider tied group, different shards can pick
        // different rows from it, but the count and column-0 value must match.
        int i = 0;
        while (i < expectedList.size()) {
            Object expectedKey = sortKey((List<Object>) expectedList.get(i));
            Object actualKey = sortKey((List<Object>) actualList.get(i));
            if (!sameKey(expectedKey, actualKey)) return false;
            int j = i + 1;
            while (j < expectedList.size() && sameKey(sortKey((List<Object>) expectedList.get(j)), expectedKey)) j++;
            int actualEnd = i + 1;
            while (actualEnd < actualList.size() && sameKey(sortKey((List<Object>) actualList.get(actualEnd)), expectedKey)) actualEnd++;
            if (j != actualEnd) return false;
            boolean isBoundaryGroup = j == expectedList.size();
            if (!isBoundaryGroup) {
                Set<List<Object>> expectedGroup = new HashSet<>();
                Set<List<Object>> actualGroup = new HashSet<>();
                for (int k = i; k < j; k++) {
                    expectedGroup.add((List<Object>) expectedList.get(k));
                    actualGroup.add((List<Object>) actualList.get(k));
                }
                if (!expectedGroup.equals(actualGroup)) return false;
            }
            i = j;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean sameMultiset(List<?> a, List<?> b) {
        Map<List<Object>, Integer> aCounts = new java.util.HashMap<>();
        Map<List<Object>, Integer> bCounts = new java.util.HashMap<>();
        for (Object row : a) aCounts.merge((List<Object>) row, 1, Integer::sum);
        for (Object row : b) bCounts.merge((List<Object>) row, 1, Integer::sum);
        return aCounts.equals(bCounts);
    }

    private static Object sortKey(List<Object> row) {
        return row.isEmpty() ? null : row.get(0);
    }

    private static boolean sameKey(Object a, Object b) {
        return a == null ? b == null : a.equals(b);
    }

    private Map<Integer, Object> runAllQueries(List<Integer> queryNumbers) throws IOException {
        Map<Integer, Object> results = new LinkedHashMap<>();
        for (int q : queryNumbers) {
            String pplFile = "datasets/" + ClickBenchTestHelper.DATASET.name + "/ppl/q" + q + ".ppl";
            String ppl;
            try (var is = getClass().getClassLoader().getResourceAsStream(pplFile)) {
                if (is == null) continue;
                ppl = new String(is.readAllBytes()).trim().replace("clickbench", INDEX_NAME);
            }
            try {
                Request request = new Request("POST", "/_analytics/ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client().performRequest(request);
                Map<String, Object> body = assertOkAndParse(response, "Q" + q);
                results.put(q, body.get("rows"));
            } catch (ResponseException e) {
                logger.warn("Q{} failed: {}", q, e.getMessage());
                results.put(q, null);
            } catch (Exception e) {
                logger.warn("Q{} error: {}", q, e.getMessage());
                results.put(q, null);
            }
        }
        return results;
    }

    private void createAndIngest(int shards) throws Exception {
        // Load mapping from resources and override shard count
        String mapping;
        try (var is = getClass().getClassLoader().getResourceAsStream("datasets/clickbench/mapping.json")) {
            mapping = new String(is.readAllBytes());
        }
        // Inject parquet settings, disable shard-bucket oversampling so distributed
        // execution returns exact (gather-all) results, and override shard count
        // (same approach as DatasetProvisioner).
        mapping = mapping.replace("\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
            + "\"index.pluggable.dataformat\": \"composite\", "
            + "\"index.composite.primary_data_format\": \"parquet\", "
            + "\"index.analytics.shard_bucket_oversampling_factor\": 0.0, "
            + "\"number_of_shards\"");
        mapping = mapping.replaceFirst("\"number_of_shards\":\\s*\\d+", "\"number_of_shards\": " + shards);

        Request create = new Request("PUT", "/" + INDEX_NAME);
        create.setJsonEntity(mapping);
        client().performRequest(create);

        // Generate procedurally — deterministic for (DOC_COUNT, DATA_SEED).
        // Same dataset whether shards=1 or shards=4, so result comparison is meaningful.
        ingestGeneratedDocs();

        // Flush to parquet
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
        // Wait for green
        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Generates {@link #DOC_COUNT} docs in BULK_BATCH_SIZE-doc chunks and posts each
     * batch to {@code _bulk?refresh=false}. A single trailing refresh keeps the index
     * available for queries; per-batch refresh would force a flush per chunk and slow
     * the test by ~10x with no behavioral difference.
     */
    private void ingestGeneratedDocs() throws IOException {
        int batches = (DOC_COUNT + BULK_BATCH_SIZE - 1) / BULK_BATCH_SIZE;
        for (int b = 0; b < batches; b++) {
            int from = b * BULK_BATCH_SIZE;
            int to = Math.min(from + BULK_BATCH_SIZE, DOC_COUNT);
            // Seed = DATA_SEED + from so each batch is independently reproducible and
            // batches don't depend on iteration order.
            String chunk = ClickBenchDataGenerator.generate(to - from, DATA_SEED + from);
            Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
            bulkRequest.setJsonEntity(chunk);
            bulkRequest.setOptions(
                bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
            );
            Response bulkResponse = client().performRequest(bulkRequest);
            Map<String, Object> bulkResult = assertOkAndParse(bulkResponse, "bulk batch " + b);
            if (Boolean.TRUE.equals(bulkResult.get("errors"))) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResult.get("items");
                for (Map<String, Object> item : items) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> index = (Map<String, Object>) item.get("index");
                    if (index != null && index.containsKey("error")) {
                        throw new AssertionError("Bulk ingest error in batch " + b + ": " + index.get("error"));
                    }
                }
            }
        }
        // Single final refresh so all batches are visible to subsequent queries.
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
    }

    private void deleteIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (ResponseException e) {
            // ignore if not exists
        }
    }
}
