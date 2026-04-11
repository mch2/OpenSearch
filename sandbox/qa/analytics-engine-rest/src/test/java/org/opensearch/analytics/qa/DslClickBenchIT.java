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
 * ClickBench integration test using DSL (Elasticsearch Query DSL) queries.
 */
public class DslClickBenchIT extends ClickBenchBaseIT {

    public void testQ1CountAll() throws Exception {
        setupIndex();

        String dslBody = loadDslQuery(1);
        logger.info("=== DSL Q1: Count All ===\n{}", dslBody);

        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        request.setJsonEntity(dslBody);
        Response response = client().performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        logger.info("DSL Q1 response: {}", responseMap);

        // Verify we got a response with hits
        @SuppressWarnings("unchecked")
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        assertNotNull("Response should contain 'hits'", hits);

        @SuppressWarnings("unchecked")
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertNotNull("Response should contain 'hits.total'", total);

        int totalHits = ((Number) total.get("value")).intValue();
        logger.info("DSL Q1 result: total hits = {}", totalHits);
//        assertEquals("Expected 100 documents", 100, totalHits);
    }
}
