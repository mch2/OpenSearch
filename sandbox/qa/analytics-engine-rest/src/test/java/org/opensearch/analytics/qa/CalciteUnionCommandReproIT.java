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
 * Reproduction of failing {@code union} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteUnionCommandIT} on the analytics-engine route.
 * Uses {@code account}.
 */
public class CalciteUnionCommandReproIT extends CalciteReproTestCase {

    private static final Dataset ACCOUNT = new Dataset("account", "repro_union_account");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), ACCOUNT);
        provisioned = true;
    }

    public void testUnionMidPipeline_SingleExplicitDataset() throws IOException {
        Map<String, Object> result = executePpl("search source=" + ACCOUNT.indexName + " | where gender = \"M\""
            + " | union [search source=" + ACCOUNT.indexName + " | where gender = \"F\"]"
            + " | stats count() as total");
        verifySchema(result, schema("total", "bigint"));
        verifyDataRows(result, rows(1000L));
    }

    public void testUnionThreeSubsearches() throws IOException {
        Map<String, Object> result = executePpl(
            "| union [search source=" + ACCOUNT.indexName + " | where state = \"IL\" | eval region = \"Illinois\"]"
            + " [search source=" + ACCOUNT.indexName + " | where state = \"TN\" | eval region = \"Tennessee\"]"
            + " [search source=" + ACCOUNT.indexName + " | where state = \"CA\" | eval region = \"California\"]"
            + " | stats count by region | sort region");
        verifySchema(result, schema("count", "bigint"), schema("region", "string"));
        verifyDataRows(result, rows(17L, "California"), rows(22L, "Illinois"), rows(25L, "Tennessee"));
    }
}
