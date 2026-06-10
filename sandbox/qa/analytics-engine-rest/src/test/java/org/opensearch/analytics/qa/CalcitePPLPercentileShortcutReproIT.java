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
import java.util.Map;

/**
 * Reproduction of the percentile-shortcut methods (perc50, p95, perc99.5, ...) from upstream
 * {@code CalcitePPLAggregationIT}/{@code CalcitePPLAggregationPaginatingIT} on the analytics-engine
 * route. Uses {@code bank}. The nested-field aggregation tests (testMinMaxWithBooleanNestedField,
 * testMixedTypesNestedFieldAggregations) are skipped per policy (they query deeply-nested
 * resource.attributes.* fields).
 */
public class CalcitePPLPercentileShortcutReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_pct_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    private String src() { return "source=" + BANK.indexName; }

    public void testPercentileShortcutsWithDecimals() throws IOException {
        Map<String, Object> actual = executePpl(src() + " | stats perc99.5(balance)");
        verifySchema(actual, schema("perc99.5(balance)", "bigint"));
        verifyDataRows(actual, rows(48086));
    }

    public void testPercentileShortcutsFloatingPoint() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | stats perc25.5(balance), p75.25(balance), perc0.1(balance)");
        verifySchema(actual,
            schema("perc25.5(balance)", "bigint"),
            schema("p75.25(balance)", "bigint"),
            schema("perc0.1(balance)", "bigint"));
        verifyDataRows(actual, rows(5686, 40540, 4180));
    }

    public void testPercentileShortcutsEquivalentToStandard() throws IOException {
        Map<String, Object> shortcut = executePpl(src() + " | stats perc50(balance)");
        Map<String, Object> standard = executePpl(src() + " | stats percentile(balance, 50)");
        verifySchema(shortcut, schema("perc50(balance)", "bigint"));
        verifySchema(standard, schema("percentile(balance, 50)", "bigint"));
        // The shortcut form must agree with the standard percentile() form.
        Object sc = dataRowsOf(shortcut).get(0).get(0);
        Object st = dataRowsOf(standard).get(0).get(0);
        assertEquals("perc50 shortcut must equal percentile(...,50)", st, sc);
    }

    public void testPercentileShortcutsFloatingEquivalence() throws IOException {
        Map<String, Object> shortcut = executePpl(src() + " | stats perc25.5(balance)");
        Map<String, Object> standard = executePpl(src() + " | stats percentile(balance, 25.5)");
        verifySchema(shortcut, schema("perc25.5(balance)", "bigint"));
        verifySchema(standard, schema("percentile(balance, 25.5)", "bigint"));
        List<List<Object>> scRows = dataRowsOf(shortcut);
        List<List<Object>> stRows = dataRowsOf(standard);
        assertEquals("perc25.5 shortcut must equal percentile(...,25.5)",
            stRows.get(0).get(0), scRows.get(0).get(0));
    }
}
