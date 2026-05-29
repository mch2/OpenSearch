/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * Big5 PPL integration test. Runs PPL translations of the opensearch-benchmark big5 workload
 * against an analytics-engine-backed (composite/parquet) index.
 *
 * <p>Query path: {@code POST /_plugins/_ppl} → opensearch-sql → analytics-engine → Calcite →
 * Substrait → DataFusion.
 *
 * <p>Query numbering matches the workload's {@code operations/default.json} order (after the
 * leading {@code index-append} bulk op). Queries that have no PPL equivalent — scroll
 * pagination ({@code scroll}), {@code search_after} cursors, Lucene-syntax {@code query_string},
 * {@code significant_terms}, composite-agg pagination — are degraded to the closest PPL form
 * (plain match_all, range gates, match(), nested stats).
 */
public class PplBig5IT extends AnalyticsRestTestCase {

    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.PASS_ON_MISSING;

    /**
     * Queries to skip. Populate as features are diagnosed; keep the list minimal so regressions
     * surface quickly. Skipping should mean a feature is genuinely missing, not just broken.
     */
    private static final Set<Integer> SKIP_QUERIES = Set.of();

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), Big5TestHelper.DATASET);
            dataProvisioned = true;
        }
    }

    public void testBig5PplQueries() throws Exception {
        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(Big5TestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());
        logger.info("Running {} PPL queries: {}", queryNumbers.size(), queryNumbers);

        List<String> failures = DatasetQueryRunner.runQueries(
            client(),
            Big5TestHelper.DATASET,
            "ppl",
            "ppl",
            queryNumbers,
            (client, dataset, queryBody) -> {
                String ppl = queryBody.trim().replace("big5", dataset.indexName);
                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
                Response response = client.performRequest(request);
                return assertOkAndParse(response, "PPL query");
            },
            STRATEGY
        );

        if (failures.isEmpty() == false) {
            fail("PPL query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n" + String.join("\n", failures));
        }
    }
}
