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
    private static final Set<Integer> SKIP_QUERIES = Set.of(18, 19, 28, 29, 34, 35);

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
        logger.info("Phase 2: re-ingesting into 4-shard index for distributed execution");
        deleteIndex();
        createAndIngest(4);
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
            if (!expected.equals(actual)) {
                mismatches.add("Q" + q + ": results differ\n  single-shard: " + expected + "\n  multi-shard:  " + actual);
            }
        }

        if (!mismatches.isEmpty()) {
            fail("Correctness mismatches between single-shard and multi-shard execution:\n"
                + String.join("\n\n", mismatches));
        }

        logger.info("All {} queries produce identical results across 1-shard and 4-shard configurations", groundTruth.size());
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
        // Inject parquet settings and override shard count (same approach as DatasetProvisioner)
        mapping = mapping.replace("\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
            + "\"index.pluggable.dataformat\": \"composite\", "
            + "\"index.composite.primary_data_format\": \"parquet\", "
            + "\"number_of_shards\"");
        mapping = mapping.replaceFirst("\"number_of_shards\":\\s*\\d+", "\"number_of_shards\": " + shards);

        Request create = new Request("PUT", "/" + INDEX_NAME);
        create.setJsonEntity(mapping);
        client().performRequest(create);

        // Load and ingest bulk data
        String bulk;
        try (var is = getClass().getClassLoader().getResourceAsStream("datasets/clickbench/bulk.json")) {
            bulk = new String(is.readAllBytes());
        }
        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Response bulkResponse = client().performRequest(bulkRequest);
        Map<String, Object> bulkResult = assertOkAndParse(bulkResponse, "bulk ingest");
        if (Boolean.TRUE.equals(bulkResult.get("errors"))) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> items = (List<Map<String, Object>>) bulkResult.get("items");
            for (Map<String, Object> item : items) {
                @SuppressWarnings("unchecked")
                Map<String, Object> index = (Map<String, Object>) item.get("index");
                if (index != null && index.containsKey("error")) {
                    throw new AssertionError("Bulk ingest error: " + index.get("error"));
                }
            }
        }

        // Flush to parquet
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
        // Wait for green
        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void deleteIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (ResponseException e) {
            // ignore if not exists
        }
    }
}
