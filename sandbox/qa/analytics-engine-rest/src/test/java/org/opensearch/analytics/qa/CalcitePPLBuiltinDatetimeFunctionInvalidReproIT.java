/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;

/**
 * Reproduction of the invalid-format error tests from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLBuiltinDatetimeFunctionInvalidIT} on the
 * analytics-engine route. Each expects a 4xx whose message contains
 * {@code "date:... in unsupported format, please use 'yyyy-MM-dd'"}. Uses {@code date_formats_null}
 * (only a source for the eval; the literal arg drives the error).
 */
public class CalcitePPLBuiltinDatetimeFunctionInvalidReproIT extends CalciteReproTestCase {

    private static final Dataset DFN = new Dataset("date_formats_null", "repro_dtinv_dfn");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DFN);
        provisioned = true;
    }

    private void assertUnsupportedDate(String fn, String badValue) throws IOException {
        String err = executePplExpectingFailure("source=" + DFN.indexName
            + " | eval a=" + fn + "('" + badValue + "') | fields a");
        verifyErrorMessageContains(err, "date:" + badValue + " in unsupported format, please use 'yyyy-MM-dd'");
    }

    public void testMONTHNAMEInvalid() throws IOException {
        assertUnsupportedDate("MONTHNAME", "2025-13-02");
        assertUnsupportedDate("MONTHNAME", "16:00:61");
        assertUnsupportedDate("MONTHNAME", "2025-12-01 15:02:61");
    }

    public void testDAYNAMEInvalid() throws IOException {
        assertUnsupportedDate("DAYNAME", "2025-13-02");
        assertUnsupportedDate("DAYNAME", "16:00:61");
        assertUnsupportedDate("DAYNAME", "2025-12-01 15:02:61");
    }
}
