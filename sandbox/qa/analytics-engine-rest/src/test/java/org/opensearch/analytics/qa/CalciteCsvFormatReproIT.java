/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Reproduction of CSV-format output tests from upstream {@code CalciteCsvFormatIT} on the
 * analytics-engine route. Issues a SQL query against {@code POST /_plugins/_sql?format=csv} and
 * checks the raw CSV body — including formula-injection sanitization (leading +,-,=,@ get a quote
 * prefix) and quoting of values containing commas. Uses {@code bank_csv_sanitize}.
 */
public class CalciteCsvFormatReproIT extends CalciteReproTestCase {

    private static final Dataset CSV = new Dataset("bank_csv_sanitize", "repro_csv_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), CSV);
        provisioned = true;
    }

    private String sql(String query, String format) throws IOException {
        Request request = new Request("POST", "/_plugins/_sql?format=" + format);
        request.setJsonEntity("{\"query\": \"" + escapeJson(query) + "\"}");
        Response response = client().performRequest(request);
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public void testSanitize() throws IOException {
        String result = sql("SELECT firstname, lastname FROM " + CSV.indexName, "csv");
        String nl = System.lineSeparator();
        assertEquals(
            "firstname,lastname" + nl
                + "'+Amber JOHnny,Duke Willmington+" + nl
                + "'-Hattie,Bond-" + nl
                + "'=Nanette,Bates=" + nl
                + "'@Dale,Adams@" + nl
                + "\",Elinor\",\"Ratliff,,,\"" + nl,
            result);
    }

    public void testEscapeSanitize() throws IOException {
        String result = sql("SELECT firstname, lastname FROM " + CSV.indexName, "csv&sanitize=false");
        String nl = System.lineSeparator();
        assertEquals(
            "firstname,lastname" + nl
                + "+Amber JOHnny,Duke Willmington+" + nl
                + "-Hattie,Bond-" + nl
                + "=Nanette,Bates=" + nl
                + "@Dale,Adams@" + nl
                + "\",Elinor\",\"Ratliff,,,\"" + nl,
            result);
    }
}
