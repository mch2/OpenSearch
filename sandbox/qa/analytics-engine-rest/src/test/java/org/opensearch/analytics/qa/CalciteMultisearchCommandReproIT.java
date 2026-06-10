/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Map;

/**
 * Reproduction of a failing {@code multisearch} method from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteMultisearchCommandIT} on the analytics-engine
 * route. Uses {@code time_test_data} (categories A=26 rows, B=25 rows).
 */
public class CalciteMultisearchCommandReproIT extends CalciteReproTestCase {

    private static final Dataset TTD = new Dataset("time_test_data", "repro_ms_ttd");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), TTD);
        provisioned = true;
    }

    public void testMultisearchWithoutFurtherProcessing() throws IOException {
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + TTD.indexName + " | where category = \"A\"]"
                + " [search source=" + TTD.indexName + " | where category = \"B\"]");
        verifySchema(result,
            schema("@timestamp", "timestamp"), schema("category", "string"),
            schema("value", "int"), schema("timestamp", "timestamp"));
        // category A (26) + category B (25) = 51 total rows
        verifyNumOfRows(result, 51);
    }
}
