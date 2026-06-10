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
 * Reproduction of failing null-propagation datetime methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLBuiltinFunctionsNullIT} on the analytics-engine
 * route. Uses {@code date_formats_null} (one all-null row: date/date_time/time) and
 * {@code state_country_null}.
 */
public class CalcitePPLBuiltinFunctionsNullReproIT extends CalciteReproTestCase {

    private static final Dataset DFN = new Dataset("date_formats_null", "repro_dfn");
    private static final Dataset SCN = new Dataset("state_country_null", "repro_bfn_scn");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DFN);
        DatasetProvisioner.provision(client(), SCN);
        provisioned = true;
    }

    public void testAddSubDateNull() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DFN.indexName
            + " | eval n1 = ADDDATE(date_time, INTERVAL 1 DAY), n2 = ADDDATE(date, 1), n3 = SUBDATE(time, 1)"
            + " | fields n1, n2, n3");
        verifySchema(actual, schema("n1", "timestamp"), schema("n2", "date"), schema("n3", "timestamp"));
        verifyDataRows(actual, rows(null, null, null));
    }

    public void testAdddateNull() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DFN.indexName
            + " | eval a1 = ADDDATE(date, 3), a2 = ADDDATE(date_time, 3) | fields a1, a2");
        verifySchema(actual, schema("a1", "date"), schema("a2", "timestamp"));
        verifyDataRows(actual, rows(null, null));
    }

    public void testSubdateNull() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DFN.indexName
            + " | eval sd1 = SUBDATE(date, 3), sd2 = SUBDATE(date_time, 5) | fields sd1, sd2");
        verifySchema(actual, schema("sd1", "date"), schema("sd2", "timestamp"));
        verifyDataRows(actual, rows(null, null));
    }

    public void testTimediffNull() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DFN.indexName
            + " | eval td1 = TIMEDIFF(time, time) | fields td1");
        verifySchema(actual, schema("td1", "time"));
        verifyDataRows(actual, rows((Object) null));
    }

    public void testDatetimeNullString() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SCN.indexName
            + " | where age = 10 | eval d1 = DATETIME(name, '+10:00'),"
            + " d2 = datetime('2004-02-28 23:00:00-10:00', state) | fields d1, d2");
        verifySchema(actual, schema("d1", "timestamp"), schema("d2", "timestamp"));
        verifyDataRows(actual, rows(null, null));
    }
}
