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
 * Reproduction of failing {@code adddate/subdate} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalciteDateTimeFunctionIT} on the analytics-engine route.
 * Uses a 2-row {@code date_kw} source (values are literal-driven; only row presence matters).
 */
public class CalciteDateTimeFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset DATE = new Dataset("date_kw", "repro_dt_date");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DATE);
        provisioned = true;
    }

    public void testAddDateWithDays() throws IOException {
        Map<String, Object> r = executePpl("source=" + DATE.indexName
            + " | eval f = adddate(date('2020-09-16'), 1) | fields f");
        verifySchema(r, schema("f", "date"));
        verifyDataRowsSome(r, rows("2020-09-17"));

        r = executePpl("source=" + DATE.indexName
            + " | eval f = adddate(timestamp('2020-09-16 17:30:00'), 1) | fields f");
        verifySchema(r, schema("f", "timestamp"));
        verifyDataRowsSome(r, rows("2020-09-17 17:30:00"));
    }

    public void testSubDateDays() throws IOException {
        Map<String, Object> r = executePpl("source=" + DATE.indexName
            + " | eval f = subdate(date('2020-09-16'), 1) | fields f");
        verifySchema(r, schema("f", "date"));
        verifyDataRowsSome(r, rows("2020-09-15"));

        r = executePpl("source=" + DATE.indexName
            + " | eval f = subdate(timestamp('2020-09-16 17:30:00'), 1) | fields f");
        verifySchema(r, schema("f", "timestamp"));
        verifyDataRowsSome(r, rows("2020-09-15 17:30:00"));
    }
}
