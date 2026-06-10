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
 * Reproduction of failing {@code bin} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteBinCommandIT} on the analytics-engine route.
 * Uses {@code time_test_data} (timestamp span bins) and {@code events_null}
 * (auto_date_histogram bins=N + stats).
 */
public class CalciteBinCommandReproIT extends CalciteReproTestCase {

    private static final Dataset TTD = new Dataset("time_test_data", "repro_bin_ttd");
    private static final Dataset EVN = new Dataset("events_null", "repro_bin_events_null");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), TTD);
        DatasetProvisioner.provision(client(), EVN);
        provisioned = true;
    }

    public void testBinTimestampSpan6Days() throws IOException {
        Map<String, Object> r = executePpl("source=" + TTD.indexName
            + " | bin @timestamp span=6day | fields @timestamp, value | sort @timestamp | head 3");
        verifySchema(r, schema("@timestamp", "timestamp"), schema("value", "int"));
        verifyDataRows(r,
            rows("2025-07-23 00:00:00", 8945),
            rows("2025-07-23 00:00:00", 7623),
            rows("2025-07-23 00:00:00", 9187));
    }

    public void testBinTimestampSpan7Days() throws IOException {
        Map<String, Object> r = executePpl("source=" + TTD.indexName
            + " | bin @timestamp span=7day | fields @timestamp, value | sort @timestamp | head 3");
        verifySchema(r, schema("@timestamp", "timestamp"), schema("value", "int"));
        verifyDataRows(r,
            rows("2025-07-24 00:00:00", 8945),
            rows("2025-07-24 00:00:00", 7623),
            rows("2025-07-24 00:00:00", 9187));
    }

    public void testStatsWithBinsOnTimeField_Count() throws IOException {
        Map<String, Object> r = executePpl("source=" + EVN.indexName
            + " | bin @timestamp bins=3 | stats count() by @timestamp");
        verifySchema(r, schema("count()", "bigint"), schema("@timestamp", "timestamp"));
        verifyDataRows(r, rows(5, "2024-07-01 00:00:00"), rows(1, "2024-07-01 00:05:00"));
    }

    public void testStatsWithBinsOnTimeField_Avg() throws IOException {
        Map<String, Object> r = executePpl("source=" + EVN.indexName
            + " | bin @timestamp bins=3 | stats avg(cpu_usage) by @timestamp");
        verifySchema(r, schema("avg(cpu_usage)", "double"), schema("@timestamp", "timestamp"));
        verifyDataRows(r, rows(44.62, "2024-07-01 00:00:00"), rows(50.0, "2024-07-01 00:05:00"));
    }

    public void testStatsWithBinsOnTimeAndTermField_Count() throws IOException {
        Map<String, Object> r = executePpl("source=" + EVN.indexName
            + " | bin @timestamp bins=3 | stats bucket_nullable=false count() by region, @timestamp");
        verifySchema(r, schema("count()", "bigint"), schema("region", "string"), schema("@timestamp", "timestamp"));
        verifyDataRows(r,
            rows(1, "eu-west", "2024-07-01 00:03:00"),
            rows(2, "us-east", "2024-07-01 00:00:00"),
            rows(1, "us-east", "2024-07-01 00:05:00"),
            rows(2, "us-west", "2024-07-01 00:01:00"));
    }

    public void testStatsWithBinsOnTimeAndTermField_Avg() throws IOException {
        Map<String, Object> r = executePpl("source=" + EVN.indexName
            + " | bin @timestamp bins=3 | stats bucket_nullable=false avg(cpu_usage) by region, @timestamp");
        verifySchema(r, schema("avg(cpu_usage)", "double"), schema("region", "string"), schema("@timestamp", "timestamp"));
        verifyDataRows(r,
            rows(42.1, "eu-west", "2024-07-01 00:03:00"),
            rows(50.25, "us-east", "2024-07-01 00:00:00"),
            rows(50, "us-east", "2024-07-01 00:05:00"),
            rows(40.25, "us-west", "2024-07-01 00:01:00"));
    }
}
