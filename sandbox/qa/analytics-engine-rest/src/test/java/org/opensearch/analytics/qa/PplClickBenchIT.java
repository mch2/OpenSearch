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

import java.util.Map;

/**
 * ClickBench integration test using PPL (Piped Processing Language) queries.
 */
public class PplClickBenchIT extends ClickBenchBaseIT {

    public void testQ1CountAll() throws Exception {
        setupIndex();

        String pplQuery = loadPplQuery(1);
        // Replace "clickbench" with actual index name
        pplQuery = pplQuery.replace("clickbench", INDEX_NAME);
        logger.info("=== PPL Q1: Count All ===\n{}", pplQuery);

        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(pplQuery) + "\"}");
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        logger.info("PPL Q1 response: {}", responseMap);

        logger.info("PPL Q1 executed successfully");
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
