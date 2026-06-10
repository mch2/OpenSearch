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
 * Reproduction of the last remaining failing tests:
 * <ul>
 *   <li>{@code CalciteLikeQueryIT#test_the_default_3rd_option} (wildcard dataset, legacy-syntax
 *       cluster setting)</li>
 *   <li>{@code CalciteWhereCommandIT#testFilterScriptPushDownWithPPLBuiltInFunction}
 *       ({@code month(login_time)=1} on a date field)</li>
 *   <li>{@code CalciteResourceMonitorIT#queryExceedResourceLimitShouldFail}
 *       ({@code plugins.query.memory_limit=1%} → query must fail with a resource error)</li>
 * </ul>
 */
public class CalciteFinalReproIT extends CalciteReproTestCase {

    private static final Dataset WILDCARD = new Dataset("wildcard", "repro_fin_wildcard");
    private static final Dataset DT = new Dataset("datetime_login", "repro_fin_datetime");
    private static final Dataset BANK = new Dataset("bank", "repro_fin_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), WILDCARD);
        DatasetProvisioner.provision(client(), DT);
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    /**
     * Legacy-syntax preferred ON: {@code Like(KeywordBody, 'test Wildcard%')} is treated
     * case-insensitively and matches the 7 lowercase rows. Reproduces the legacy-setting behavior.
     */
    public void testTheDefault3rdOption() throws IOException {
        setClusterSetting("plugins.ppl.syntax.legacy.preferred", "true");
        try {
            Map<String, Object> result = executePpl("source=" + WILDCARD.indexName
                + " | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody");
            verifyDataRows(result,
                rows("test wildcard"),
                rows("test wildcard in the end of the text%"),
                rows("test wildcard in % the middle of the text"),
                rows("test wildcard %% beside each other"),
                rows("test wildcard in the end of the text_"),
                rows("test wildcard in _ the middle of the text"),
                rows("test wildcard __ beside each other"));
        } finally {
            setClusterSetting("plugins.ppl.syntax.legacy.preferred", null);
        }
    }

    public void testFilterScriptPushDownWithPPLBuiltInFunction() throws IOException {
        Map<String, Object> actual = executePpl("source=" + DT.indexName
            + " | where month(login_time) = 1");
        verifySchema(actual, schema("birthday", "timestamp"), schema("login_time", "timestamp"));
        verifyDataRows(actual,
            rows(null, "2015-01-01 00:00:00"),
            rows(null, "2015-01-01 12:10:30"),
            rows(null, "1970-01-19 08:31:22.955"));
    }

    public void testQueryExceedResourceLimitShouldFail() throws IOException {
        setClusterSetting("plugins.query.memory_limit", "1%");
        try {
            String err = executePplExpectingFailure("source=" + BANK.indexName);
            verifyErrorMessageContains(err, "Insufficient resources to");
            verifyErrorMessageContains(err, "plugins.query.memory_limit");
        } finally {
            setClusterSetting("plugins.query.memory_limit", null);
        }
    }

    private void setClusterSetting(String key, String value) throws IOException {
        org.opensearch.client.Request req =
            new org.opensearch.client.Request("PUT", "/_cluster/settings");
        String v = value == null ? "null" : "\"" + value + "\"";
        req.setJsonEntity("{\"transient\":{\"" + key + "\":" + v + "}}");
        client().performRequest(req);
    }
}
