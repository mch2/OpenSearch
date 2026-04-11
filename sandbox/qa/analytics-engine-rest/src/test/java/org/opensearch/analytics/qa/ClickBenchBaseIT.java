/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * Common base class for ClickBench integration tests.
 * Creates the "parquet_hits" index with ClickBench mapping and ingests
 * 100 documents from bulk.json (matching clickbench_hits_100.parquet).
 */
public abstract class ClickBenchBaseIT extends OpenSearchRestTestCase {

    protected static final Logger logger = LogManager.getLogger(ClickBenchBaseIT.class);
    protected static final String INDEX_NAME = "parquet_hits";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    protected void setupIndex() throws IOException {
        // Delete if exists
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception e) {
            // ignore
        }

        // Create index with mapping
        String mapping = loadResource("clickbench/parquet_hits_mapping.json");
        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(mapping);
        client().performRequest(createIndex);

        // Bulk ingest
        String bulkBody = loadResource("clickbench/bulk.json");
        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder()
            .addHeader("Content-Type", "application/x-ndjson")
            .build());
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());

        // Wait for index health
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "yellow");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);

        logger.info("Index [{}] created with 100 documents", INDEX_NAME);
    }

    protected String loadResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }

    protected String loadDslQuery(int queryNumber) throws IOException {
        return loadResource("clickbench/dsl/q" + queryNumber + ".json");
    }

    protected String loadPplQuery(int queryNumber) throws IOException {
        return loadResource("clickbench/ppl/q" + queryNumber + ".ppl").trim();
    }
}
