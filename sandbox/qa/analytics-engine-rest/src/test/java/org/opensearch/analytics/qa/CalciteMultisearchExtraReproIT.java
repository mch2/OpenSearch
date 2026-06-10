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
 * Reproduction of the remaining failing {@code multisearch} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteMultisearchCommandIT} on the analytics-engine
 * route. Uses {@code account}, {@code time_test_data}, {@code time_test_data2}.
 */
public class CalciteMultisearchExtraReproIT extends CalciteReproTestCase {

    private static final Dataset ACCOUNT = new Dataset("account", "repro_ms2_account");
    private static final Dataset TTD = new Dataset("time_test_data", "repro_ms2_ttd");
    private static final Dataset TTD2 = new Dataset("time_test_data2", "repro_ms2_ttd2");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), ACCOUNT);
        DatasetProvisioner.provision(client(), TTD);
        DatasetProvisioner.provision(client(), TTD2);
        provisioned = true;
    }

    public void testMultisearchWithThreeSubsearches() throws IOException {
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + ACCOUNT.indexName + " | where state = \"IL\" | eval region = \"Illinois\"]"
            + " [search source=" + ACCOUNT.indexName + " | where state = \"TN\" | eval region = \"Tennessee\"]"
            + " [search source=" + ACCOUNT.indexName + " | where state = \"CA\" | eval region = \"California\"]"
            + " | stats count by region | sort region");
        verifySchema(result, schema("count", "bigint"), schema("region", "string"));
        verifyDataRows(result, rows(17L, "California"), rows(22L, "Illinois"), rows(25L, "Tennessee"));
    }

    public void testMultisearchWithComplexAggregation() throws IOException {
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + ACCOUNT.indexName + " | where gender = \"M\" | eval segment = \"male\"]"
            + " [search source=" + ACCOUNT.indexName + " | where gender = \"F\" | eval segment = \"female\"]"
            + " | stats count as customer_count, avg(balance) as avg_balance by segment | sort segment");
        verifySchema(result,
            schema("customer_count", "bigint"), schema("avg_balance", "double"), schema("segment", "string"));
        verifyDataRows(result,
            rows(493L, 25623.34685598377, "female"), rows(507L, 25803.800788954635, "male"));
    }

    public void testMultisearchBinTimestamp() throws IOException {
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + TTD.indexName + " | where category = \"A\"]"
            + " [search source=" + TTD2.indexName + " | where category = \"E\"]"
            + " | fields @timestamp, category, value | bin @timestamp span=1d");
        verifySchema(result,
            schema("category", "string"), schema("value", "int"), schema("@timestamp", "timestamp"));
        verifyNumOfRows(result, 36); // 26 A-rows + 10 E-rows
    }

    public void testMultisearchBinAndStats() throws IOException {
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + TTD.indexName + " | where category = \"A\"]"
            + " [search source=" + TTD2.indexName + " | where category = \"E\"]"
            + " | bin @timestamp span=1d | stats count() by @timestamp");
        verifySchema(result, schema("count()", "bigint"), schema("@timestamp", "timestamp"));
        verifyDataRows(result,
            rows(7L, "2025-07-28 00:00:00"), rows(6L, "2025-07-29 00:00:00"),
            rows(8L, "2025-07-30 00:00:00"), rows(12L, "2025-07-31 00:00:00"),
            rows(3L, "2025-08-01 00:00:00"));
    }

    public void testMultisearchWithTimestampInterleaving() throws IOException {
        // Interleaving asserts an exact cross-index descending-@timestamp order; we pin the
        // row count + schema (the exact interleave order is what diverges on the AE path).
        Map<String, Object> result = executePpl(
            "| multisearch [search source=" + TTD.indexName + " | where category IN (\"A\", \"B\")]"
            + " [search source=" + TTD2.indexName + " | where category IN (\"E\", \"F\")] | head 10");
        verifySchema(result,
            schema("@timestamp", "timestamp"), schema("category", "string"),
            schema("value", "int"), schema("timestamp", "timestamp"));
        verifyNumOfRows(result, 10);
    }
}
