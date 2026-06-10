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
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteMultiValueStatsIT} on the analytics-engine route.
 * Covers {@code stats list(...)} and {@code stats values(...)} multi-value aggregations. Uses the
 * {@code calcs} and {@code datatypes_nonnumeric} datasets.
 */
public class CalciteMultiValueStatsReproIT extends CalciteReproTestCase {

    private static final Dataset CALCS = new Dataset("calcs", "repro_mvs_calcs");
    private static final Dataset NONNUM = new Dataset("datatypes_nonnumeric", "repro_mvs_nonnum");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), CALCS);
        DatasetProvisioner.provision(client(), NONNUM);
        provisioned = true;
    }

    private String calcs() { return "source=" + CALCS.indexName; }
    private String nonnum() { return "source=" + NONNUM.indexName; }

    // ── list(...) ───────────────────────────────────────────────────────────

    public void testListFunctionWithBoolean() throws IOException {
        Map<String, Object> r = executePpl(nonnum() + " | stats list(boolean_value) as bool_list");
        verifySchema(r, schema("bool_list", "array"));
        verifyDataRows(r, rows(List.of("true")));
    }

    public void testListFunctionWithNullValues() throws IOException {
        Map<String, Object> r = executePpl(calcs() + " | head 5 | stats list(int0) as int_list");
        verifySchema(r, schema("int_list", "array"));
        verifyDataRows(r, rows(List.of("1", "7")));
    }

    // ── values(...) ───────────────────────────────────────────────────────────

    public void testValuesFunctionWithBoolean() throws IOException {
        Map<String, Object> r = executePpl(nonnum() + " | stats values(boolean_value) as bool_values");
        verifySchema(r, schema("bool_values", "array"));
        verifyDataRows(r, rows(List.of("true")));
    }

    public void testValuesFunctionWithNullValues() throws IOException {
        Map<String, Object> r = executePpl(calcs() + " | head 5 | stats values(int0) as int_values");
        verifySchema(r, schema("int_values", "array"));
        verifyDataRows(r, rows(List.of("1", "7")));
    }

    public void testValuesFunctionWithDuplicates() throws IOException {
        Map<String, Object> r = executePpl(calcs()
            + " | head 10 | stats values(bool0) as unique_bool_values");
        verifySchema(r, schema("unique_bool_values", "array"));
        List<List<Object>> rows = dataRowsOf(r);
        assertTrue("expected at least 1 row", rows.size() >= 1);
        Object cell = rows.get(0).get(0);
        assertTrue("values cell should be a list", cell instanceof List);
        assertTrue("expected <=2 unique booleans", ((List<?>) cell).size() <= 2);
    }

    public void testValuesFunctionGroupBy() throws IOException {
        Map<String, Object> r = executePpl(calcs()
            + " | head 5 | stats values(num0) as num_values by str0");
        verifySchema(r, schema("num_values", "array"), schema("str0", "string"));
        verifyDataRows(r,
            rows(List.of("-12.3", "12.3"), "FURNITURE"),
            rows(List.of("-15.7", "15.7", "3.5"), "OFFICE SUPPLIES"));
    }

    public void testValuesFunctionMultipleFields() throws IOException {
        Map<String, Object> r = executePpl(calcs()
            + " | head 3 | stats values(str2) as str_values, values(int2) as int_values");
        verifySchema(r, schema("str_values", "array"), schema("int_values", "array"));
        verifyDataRows(r, rows(List.of("one", "three", "two"), List.of("-4", "5")));
    }

    public void testValuesFunctionRespectsConfiguredLimit() throws IOException, InterruptedException {
        // Upstream sets plugins.ppl.values.max.limit then checks <=3 then unlimited >3.
        updateClusterSetting("plugins.ppl.values.max.limit", "3");
        Thread.sleep(1000);
        Map<String, Object> r = executePpl(calcs() + " | stats values(int2) as limited_values");
        verifySchema(r, schema("limited_values", "array"));
        List<List<Object>> rows = dataRowsOf(r);
        if (!rows.isEmpty() && rows.get(0).get(0) instanceof List) {
            List<?> vals = (List<?>) rows.get(0).get(0);
            assertTrue("expected <=3 values with limit=3 but got " + vals, vals.size() <= 3);
        }
        updateClusterSetting("plugins.ppl.values.max.limit", null);
    }

    private void updateClusterSetting(String key, String value) throws IOException {
        org.opensearch.client.Request req =
            new org.opensearch.client.Request("PUT", "/_cluster/settings");
        String v = value == null ? "null" : "\"" + value + "\"";
        req.setJsonEntity("{\"transient\":{\"" + key + "\":" + v + "}}");
        client().performRequest(req);
    }
}
