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
 * Reproduction of failing {@code case(...)} methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT} on the analytics-engine route.
 * Uses {@code weblogs} (+ 4 appended bad-response docs 7-10) and {@code bank}.
 */
public class CalcitePPLCaseFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset WEB = new Dataset("weblogs", "repro_case_weblogs");
    private static final Dataset BANK = new Dataset("bank", "repro_case_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), WEB);
        DatasetProvisioner.provision(client(), BANK);
        // appendDataForBadResponse(): 4 docs with non-200 responses (incl. one null).
        indexDoc(WEB.indexName, "7", "{\"host\":\"::1\",\"method\":\"GET\",\"url\":\"/history/apollo/\",\"response\":\"301\",\"bytes\":\"6245\"}");
        indexDoc(WEB.indexName, "8", "{\"host\":\"0.0.0.2\",\"method\":\"GET\",\"url\":\"/shuttle/missions/sts-73/mission-sts-73.html\",\"response\":\"500\",\"bytes\":\"4085\"}");
        indexDoc(WEB.indexName, "9", "{\"host\":\"::3\",\"method\":\"GET\",\"url\":\"/shuttle/countdown/countdown.html\",\"response\":\"403\",\"bytes\":\"3985\"}");
        indexDoc(WEB.indexName, "10", "{\"host\":\"1.2.3.5\",\"method\":\"GET\",\"url\":\"/history/voyager2/\",\"response\":null,\"bytes\":\"4321\"}");
        provisioned = true;
    }

    private String web() { return "source=" + WEB.indexName; }

    public void testCaseWhenNoElse() throws IOException {
        Map<String, Object> actual = executePpl(web()
            + "| eval status = case("
            + " cast(response as int) >= 200 AND cast(response as int) < 300, 'Success',"
            + " cast(response as int) >= 300 AND cast(response as int) < 400, 'Redirection',"
            + " cast(response as int) >= 400 AND cast(response as int) < 500, 'Client Error',"
            + " cast(response as int) >= 500 AND cast(response as int) < 600, 'Server Error')"
            + "| where isnull(status) OR status != 'Success'"
            + "| fields host, method, message, bytes, response, url, status");
        verifySchema(actual,
            schema("host", "ip"), schema("method", "string"), schema("message", "string"),
            schema("url", "string"), schema("response", "string"), schema("bytes", "string"),
            schema("status", "string"));
        verifyDataRows(actual,
            rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
            rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html", "Server Error"),
            rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
            rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/", null));
    }

    public void testCaseWhenWithCast() throws IOException {
        Map<String, Object> actual = executePpl(web()
            + "| eval status = case("
            + " cast(response as int) >= 200 AND cast(response as int) < 300, 'Success',"
            + " cast(response as int) >= 300 AND cast(response as int) < 400, 'Redirection',"
            + " cast(response as int) >= 400 AND cast(response as int) < 500, 'Client Error',"
            + " cast(response as int) >= 500 AND cast(response as int) < 600, 'Server Error'"
            + " else concat('Incorrect HTTP status code for', url))"
            + "| where status != 'Success'"
            + "| fields host, method, message, bytes, response, url, status");
        verifyDataRows(actual,
            rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
            rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html", "Server Error"),
            rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
            rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/", "Incorrect HTTP status code for/history/voyager2/"));
    }

    public void testCaseWhenWithIn() throws IOException {
        Map<String, Object> actual = executePpl(web()
            + "| eval status = case("
            + " response in ('200'), 'Success',"
            + " response in ('300', '301'), 'Redirection',"
            + " response in ('400', '403'), 'Client Error',"
            + " response in ('500', '505'), 'Server Error'"
            + " else concat('Incorrect HTTP status code for', url))"
            + "| where status != 'Success'"
            + "| fields host, method, message, bytes, response, url, status");
        verifyDataRows(actual,
            rows("::1", "GET", null, "6245", "301", "/history/apollo/", "Redirection"),
            rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html", "Server Error"),
            rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html", "Client Error"),
            rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/", "Incorrect HTTP status code for/history/voyager2/"));
    }

    public void testCaseWhenInFilter() throws IOException {
        Map<String, Object> actual = executePpl(web()
            + "| where not true = case("
            + " response in ('200'), true,"
            + " response in ('300', '301'), false,"
            + " response in ('400', '403'), false,"
            + " response in ('500', '505'), false"
            + " else false)"
            + "| fields host, method, message, bytes, response, url");
        verifyDataRows(actual,
            rows("::1", "GET", null, "6245", "301", "/history/apollo/"),
            rows("0.0.0.2", "GET", null, "4085", "500", "/shuttle/missions/sts-73/mission-sts-73.html"),
            rows("::3", "GET", null, "3985", "403", "/shuttle/countdown/countdown.html"),
            rows("1.2.3.5", "GET", null, "4321", null, "/history/voyager2/"));
    }

    public void testCaseCanBePushedDownAsRangeQuery() throws IOException {
        Map<String, Object> actual = executePpl("source=" + BANK.indexName
            + " | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100')"
            + " | stats avg(age) as avg_age by age_range");
        verifySchema(actual, schema("avg_age", "double"), schema("age_range", "string"));
        verifyDataRows(actual, rows(28.0, "u30"), rows(35.0, "u40"));
    }
}
