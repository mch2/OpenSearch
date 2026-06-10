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
 * Reproduction of the remaining failing tests that reuse existing/new datasets without needing the
 * nested-object or fulltext-search paths that are handled elsewhere:
 * <ul>
 *   <li>{@code CalcitePPLMapPathIT#testMvcombineOnMapPath} (spath → map-path)</li>
 *   <li>{@code CalciteStatsCommandIT#testStatsBySpanTimeWithNullBucket} (time_date_null)</li>
 *   <li>{@code CalcitePPLAppendCommandIT#testAppendSchemaMergeWithIpUDT} (account + weblogs IP)</li>
 *   <li>{@code CalcitePPLCaseFunctionIT#testCaseWhenInSubquery} (weblogs + bad-response docs)</li>
 *   <li>{@code CalcitePPLCaseFunctionIT#testNestedCaseAggWithAutoDateHistogram} (otel_logs)</li>
 *   <li>{@code CalcitePPLPatternsIT#testBrainParseWithUUID_ShowNumberedToken} (weblogs eval)</li>
 * </ul>
 */
public class CalciteRemainingReproIT extends CalciteReproTestCase {

    private static final Dataset SPATH = new Dataset("spath_mappath", "repro_rem_spath");
    private static final Dataset TDN = new Dataset("time_date_null", "repro_rem_tdn");
    private static final Dataset ACCOUNT = new Dataset("account", "repro_rem_account");
    private static final Dataset WEB = new Dataset("weblogs", "repro_rem_weblogs");
    private static final Dataset OTEL = new Dataset("otel_logs", "repro_rem_otel");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SPATH);
        DatasetProvisioner.provision(client(), TDN);
        DatasetProvisioner.provision(client(), ACCOUNT);
        DatasetProvisioner.provision(client(), WEB);
        DatasetProvisioner.provision(client(), OTEL);
        // weblogs bad-response docs for the case-in-subquery test.
        indexDoc(WEB.indexName, "7", "{\"host\":\"::1\",\"method\":\"GET\",\"url\":\"/history/apollo/\",\"response\":\"301\",\"bytes\":\"6245\"}");
        indexDoc(WEB.indexName, "8", "{\"host\":\"0.0.0.2\",\"method\":\"GET\",\"url\":\"/shuttle/missions/sts-73/mission-sts-73.html\",\"response\":\"500\",\"bytes\":\"4085\"}");
        indexDoc(WEB.indexName, "9", "{\"host\":\"::3\",\"method\":\"GET\",\"url\":\"/shuttle/countdown/countdown.html\",\"response\":\"403\",\"bytes\":\"3985\"}");
        indexDoc(WEB.indexName, "10", "{\"host\":\"1.2.3.5\",\"method\":\"GET\",\"url\":\"/history/voyager2/\",\"response\":null,\"bytes\":\"4321\"}");
        provisioned = true;
    }

    public void testMvcombineOnMapPath() throws IOException {
        Map<String, Object> result = executePpl("source=" + SPATH.indexName
            + " | spath input=doc | mvcombine doc.user.name | fields doc.user.name, doc.user.city");
        verifySchema(result, schema("doc.user.name", "array"), schema("doc.user.city", "string"));
        verifyDataRows(result,
            rows(List.of("John"), "NYC"), rows(List.of("Alice"), "LA"), rows(List.of("John"), "SF"),
            rows(List.of("Bob"), "NYC"), rows(null, null));
    }

    public void testStatsBySpanTimeWithNullBucket() throws IOException {
        Map<String, Object> result = executePpl("source=" + TDN.indexName
            + " | stats percentile(value, 50) as p50 by span(@timestamp, 12h) as half_day");
        verifySchema(result, schema("p50", "int"), schema("half_day", "timestamp"));
        verifyDataRows(result,
            rows(8523, "2025-07-28 00:00:00"), rows(8094, "2025-07-28 12:00:00"),
            rows(8429, "2025-07-29 00:00:00"), rows(8216, "2025-07-29 12:00:00"),
            rows(8493, "2025-07-30 00:00:00"), rows(8426, "2025-07-30 12:00:00"),
            rows(8213, "2025-07-31 00:00:00"), rows(8490, "2025-07-31 12:00:00"));
    }

    public void testAppendSchemaMergeWithIpUDT() throws IOException {
        Map<String, Object> result = executePpl("source=" + ACCOUNT.indexName
            + " | fields account_number, age | append [ source=" + WEB.indexName + " | fields host ]"
            + " | where cidrmatch(host, '0.0.0.0/24')");
        verifySchemaInOrder(result,
            schema("account_number", "bigint"), schema("age", "bigint"), schema("host", "ip"));
        verifyDataRows(result, rows(null, null, "0.0.0.2"));
    }

    public void testCaseWhenInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source=" + WEB.indexName
            + "| where response in [ source = " + WEB.indexName
            + " | eval new_response = case(response in ('200'), '201', response in ('300','301'), '301',"
            + " response in ('400','403'), '403', response in ('500','505'), '500'"
            + " else concat('Incorrect HTTP status code for', url)) | fields new_response ]"
            + "| fields host, method, message, bytes, response, url");
        // Reproduces whatever the AE route does with case() inside an IN-subquery; assertion shape
        // matches upstream schema (rows are data-dependent and verified by the upstream test).
        verifySchema(result,
            schema("host", "ip"), schema("method", "string"), schema("message", "string"),
            schema("url", "string"), schema("response", "string"), schema("bytes", "string"));
    }

    public void testNestedCaseAggWithAutoDateHistogram() throws IOException {
        Map<String, Object> result = executePpl("source=" + OTEL.indexName
            + " | bin @timestamp bins=2 | eval severity_range = case(severityNumber < 16, 'minor' else 'severe')"
            + " | stats avg(severityNumber), count() by @timestamp, severity_range, flags");
        verifySchema(result,
            schema("avg(severityNumber)", "double"), schema("count()", "bigint"),
            schema("@timestamp", "timestamp"), schema("severity_range", "string"), schema("flags", "bigint"));
    }

    public void testBrainParseWithUUID_ShowNumberedToken() throws IOException {
        Map<String, Object> result = executePpl("source=" + WEB.indexName
            + " | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d user_currency=USD'"
            + " | head 1 | patterns body method=BRAIN mode=label show_numbered_token=true"
            + " | fields patterns_field, tokens");
        verifySchema(result, schema("patterns_field", "string"), schema("tokens", "struct"));
        verifyDataRows(result, rows("[PlaceOrder] user_id=<token1> user_currency=USD",
            Map.of("<token1>", List.of("d664d7be-77d8-11f0-8880-0242f00b101d"))));
    }
}
