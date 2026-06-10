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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLAggregationIT} on the analytics-engine route:
 * {@code distinct_count_approx} and the percentile shortcut functions ({@code perc50}, {@code p95},
 * ...). Uses {@code bank}.
 */
public class CalcitePPLAggregationReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_agg_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    public void testCountDistinctApprox() throws IOException {
        Map<String, Object> actual = executePpl("source=" + BANK.indexName
            + " | stats distinct_count_approx(state) by gender");
        verifySchema(actual, schema("gender", "string"), schema("distinct_count_approx(state)", "bigint"));
        verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
    }

    public void testCountDistinctApproxWithAlias() throws IOException {
        Map<String, Object> actual = executePpl("source=" + BANK.indexName
            + " | stats distinct_count_approx(state) as dca by gender");
        verifySchema(actual, schema("gender", "string"), schema("dca", "bigint"));
        verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
    }

    public void testPercentileShortcuts() throws IOException {
        Map<String, Object> actual = executePpl("source=" + BANK.indexName
            + " | stats perc50(balance), p95(balance)");
        verifySchema(actual, schema("perc50(balance)", "bigint"), schema("p95(balance)", "bigint"));
        verifyDataRows(actual, rows(32838, 48086));
    }
}
