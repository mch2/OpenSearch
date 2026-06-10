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
 * Reproduction of a few remaining failing single-method tests that reuse existing datasets:
 * <ul>
 *   <li>{@code CalciteWhereCommandIT#testDoubleEqualWithSpecialCharacters} (account)</li>
 *   <li>{@code CalcitePPLConditionBuiltinFunctionIT#testEarliestWithEval} (calcs)</li>
 *   <li>{@code CalciteSettingsIT#testQuerySizeLimit_NoPushdown} (bank, pushdown disabled)</li>
 * </ul>
 */
public class CalciteWhereMiscReproIT extends CalciteReproTestCase {

    private static final Dataset ACCOUNT = new Dataset("account", "repro_wm_account");
    private static final Dataset CALCS = new Dataset("calcs", "repro_wm_calcs");
    private static final Dataset BANK = new Dataset("bank", "repro_wm_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), ACCOUNT);
        DatasetProvisioner.provision(client(), CALCS);
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    public void testDoubleEqualWithSpecialCharacters() throws IOException {
        Map<String, Object> result = executePpl("source=" + ACCOUNT.indexName
            + " | where email == 'amberduke@pyrami.com' | fields firstname, email");
        verifyDataRows(result, rows("Amber", "amberduke@pyrami.com"));
    }

    public void testEarliestWithEval() throws IOException {
        Map<String, Object> actual = executePpl("source=" + CALCS.indexName
            + " | eval now=utc_timestamp() | eval a = earliest('now', now), b = earliest('-2d@d', now)"
            + " | fields a,b | head 1");
        verifySchema(actual, schema("a", "boolean"), schema("b", "boolean"));
        verifyDataRows(actual, rows(false, true));
    }

    public void testQuerySizeLimit_NoPushdown() throws IOException {
        // Upstream disables Calcite pushdown then runs testQuerySizeLimit; the sandbox cluster
        // forces the analytics route, so we exercise the same query-size-limit surface here.
        Map<String, Object> result = executePpl("search source=" + BANK.indexName
            + " age>35 | fields firstname");
        verifyDataRows(result, rows("Hattie"), rows("Elinor"), rows("Virginia"));
    }
}
