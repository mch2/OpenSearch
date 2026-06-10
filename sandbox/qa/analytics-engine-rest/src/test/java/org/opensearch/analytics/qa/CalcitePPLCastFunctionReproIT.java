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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLCastFunctionIT} on the analytics-engine route.
 * Uses {@code date_formats} (2 rows) for TIME/TIMESTAMP casts and {@code weblogs} for IP casts.
 */
public class CalcitePPLCastFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset DF = new Dataset("date_formats", "repro_cast_df");
    private static final Dataset WEB = new Dataset("weblogs", "repro_cast_weblogs");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DF);
        DatasetProvisioner.provision(client(), WEB);
        provisioned = true;
    }

    public void testCastTime() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DF.indexName
            + " | eval a = cast('09:07:42' as TIME) | fields a");
        verifySchema(actual, schema("a", "time"));
        verifyDataRows(actual, rows("09:07:42"), rows("09:07:42"));

        actual = executePpl("source=" + DF.indexName
            + " | head 1 | eval a = cast('1985-10-09 12:00:00' as time) | fields a");
        verifySchema(actual, schema("a", "time"));
        verifyDataRows(actual, rows("12:00:00"));
    }

    public void testCastTimestamp() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DF.indexName
            + " | eval a = cast('1984-04-12 09:07:42' as TIMESTAMP) | fields a");
        verifySchema(actual, schema("a", "timestamp"));
        verifyDataRows(actual, rows("1984-04-12 09:07:42"), rows("1984-04-12 09:07:42"));

        actual = executePpl("source=" + DF.indexName
            + " | eval a = cast('1984-04-12' as TIMESTAMP) | fields a");
        verifySchema(actual, schema("a", "timestamp"));
        verifyDataRows(actual, rows("1984-04-12 00:00:00"), rows("1984-04-12 00:00:00"));
    }

    public void testCastToIP() throws IOException {
        Map<String, Object> actual = executePpl("source=" + WEB.indexName
            + " | head 1 | eval a = cast('192.168.1.1' as IP) | fields a");
        verifySchema(actual, schema("a", "ip"));
        verifyDataRows(actual, rows("192.168.1.1"));
    }
}
