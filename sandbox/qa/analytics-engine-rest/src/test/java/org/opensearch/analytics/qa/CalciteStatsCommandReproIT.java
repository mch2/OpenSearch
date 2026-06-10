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
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteStatsCommandIT} on the analytics-engine route.
 * Focuses on percentile aggregations (with/without null buckets, by span). Uses {@code bank} and
 * {@code bank_null}.
 */
public class CalciteStatsCommandReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_stats_bank");
    private static final Dataset BANK_NULL = new Dataset("bank_null", "repro_stats_bank_null");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        DatasetProvisioner.provision(client(), BANK_NULL);
        provisioned = true;
    }

    public void testStatsPercentileWithNull() throws IOException {
        Map<String, Object> r = executePpl("source=" + BANK_NULL.indexName
            + " | stats percentile(balance, 50)");
        verifySchema(r, schema("percentile(balance, 50)", "bigint"));
        verifyDataRows(r, rows(39225));
    }

    public void testStatsPercentileBySpan() throws IOException {
        Map<String, Object> r = executePpl("source=" + BANK.indexName
            + " | stats percentile(balance, 50) as p50 by span(age, 10) as age_bucket");
        verifySchema(r, schema("p50", "bigint"), schema("age_bucket", "int"));
        verifyDataRows(r, rows(32838, 20), rows(39225, 30));
    }

    public void testStatsPercentileByNullValue() throws IOException {
        Map<String, Object> r = executePpl("source=" + BANK_NULL.indexName
            + " | stats percentile(balance, 50) as p50 by age");
        verifySchema(r, schema("p50", "bigint"), schema("age", "int"));
        verifyDataRows(r,
            rows(0, null), rows(32838, 28), rows(39225, 32), rows(4180, 33), rows(48086, 34), rows(0, 36));
    }

    public void testStatsPercentileByNullValueNonNullBucket() throws IOException {
        Map<String, Object> r = executePpl("source=" + BANK_NULL.indexName
            + " | stats bucket_nullable=false percentile(balance, 50) as p50 by age");
        verifySchema(r, schema("p50", "bigint"), schema("age", "int"));
        verifyDataRows(r,
            rows(32838, 28), rows(39225, 32), rows(4180, 33), rows(48086, 34), rows(0, 36));
    }

    public void testStatsWithLimit() throws IOException {
        Map<String, Object> r = executePpl("source=" + BANK_NULL.indexName
            + " | stats avg(balance) as a by age | head 5");
        verifySchema(r, schema("a", "double"), schema("age", "int"));
        verifyNumOfRows(r, 5);
    }
}
