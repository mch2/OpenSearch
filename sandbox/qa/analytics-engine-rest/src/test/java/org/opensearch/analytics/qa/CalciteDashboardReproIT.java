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
 * Reproduction of the WAF / NFW PPL dashboard tests from upstream
 * {@code org.opensearch.sql.ppl.dashboard.{WafPplDashboardIT,NfwPplDashboardIT}} on the
 * analytics-engine route. Uses {@code waf_logs} / {@code nfw_logs} (100 docs each).
 *
 * <p>Several of these aggregate by nested-object subfields ({@code httpRequest.uri},
 * {@code event.dest_ip}); on the AE route those are flattened/handled differently (see buckets
 * O/Y), so they are expected to diverge. {@code testTotalBlockedRequests} uses only the top-level
 * {@code action} keyword and is the cleanest signal.
 */
public class CalciteDashboardReproIT extends CalciteReproTestCase {

    private static final Dataset WAF = new Dataset("waf_logs", "repro_waf_logs");
    private static final Dataset NFW = new Dataset("nfw_logs", "repro_nfw_logs");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), WAF);
        DatasetProvisioner.provision(client(), NFW);
        provisioned = true;
    }

    // ── WAF ───────────────────────────────────────────────────────────────────

    public void testTotalBlockedRequests() throws IOException {
        Map<String, Object> r = executePpl("source=" + WAF.indexName + " | WHERE action = \"BLOCK\" | STATS count()");
        verifySchema(r, schema("count()", "bigint"));
        verifyDataRows(r, rows(21));
    }

    public void testTopRequestURIs() throws IOException {
        Map<String, Object> r = executePpl("source=" + WAF.indexName
            + " | stats count() as Count by `httpRequest.uri` | sort - Count | head 10");
        verifySchema(r, schema("Count", "bigint"), schema("httpRequest.uri", "string"));
        verifyDataRows(r,
            rows(5, "/api/v2/search"), rows(5, "/account"), rows(4, "/products"), rows(4, "/css/style.css"),
            rows(3, "/test"), rows(3, "/download"), rows(3, "/docs"), rows(3, "/billing"),
            rows(3, "/api/v2/users"), rows(2, "/about"));
    }

    // ── NFW ───────────────────────────────────────────────────────────────────

    public void testTopBlockedDestinationIPs() throws IOException {
        Map<String, Object> r = executePpl("source=" + NFW.indexName
            + " | WHERE `event.alert.action` = \"blocked\" | STATS COUNT() as Count by `event.dest_ip`"
            + " | SORT - Count | HEAD 10");
        verifySchema(r, schema("Count", "bigint"), schema("event.dest_ip", "string"));
        verifyDataRows(r,
            rows(2L, "8.8.8.8"), rows(1L, "54.146.42.172"), rows(1L, "54.242.115.112"), rows(1L, "52.216.211.88"));
    }
}
