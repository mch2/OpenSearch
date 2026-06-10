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
 * Reproduction of failing string/math methods from upstream
 * {@code CalcitePPLStringBuiltinFunctionIT} (trim/rtrim) and {@code CalciteMathematicalFunctionIT}
 * (rand/conv) on the analytics-engine route. Uses {@code state_country} and {@code bank}.
 */
public class CalcitePPLStringMathReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country", "repro_sm_sc");
    private static final Dataset BANK = new Dataset("bank", "repro_sm_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        DatasetProvisioner.provision(client(), BANK);
        // prepareTrim(): three Jim rows with leading/trailing spaces.
        indexDoc(SC.indexName, "5", "{\"name\":\"   Jim\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
        indexDoc(SC.indexName, "6", "{\"name\":\"Jim   \",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
        indexDoc(SC.indexName, "7", "{\"name\":\"   Jim   \",\"age\":70,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
        provisioned = true;
    }

    public void testTrim() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SC.indexName
            + " | where Trim(name) = 'Jim' | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "int"));
        verifyDataRows(actual, rows("   Jim", 27), rows("Jim   ", 57), rows("   Jim   ", 70));
    }

    public void testRTrim() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SC.indexName
            + " | where RTrim(name) = 'Jim' | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "int"));
        verifyDataRows(actual, rows("Jim   ", 57));
    }

    public void testRand() throws IOException {
        Map<String, Object> result = executePpl("source=" + BANK.indexName + " | eval f = rand() | fields f");
        verifySchema(result, schema("f", "double"));
        result = executePpl("source=" + BANK.indexName + " | eval f = rand(5) | fields f");
        verifySchema(result, schema("f", "double"));
    }

    public void testConv() throws IOException {
        Map<String, Object> result = executePpl("source=" + BANK.indexName
            + " | eval f = conv(age, 10, 16) | fields f");
        verifySchema(result, schema("f", "string"));
        verifyDataRows(result,
            rows("20"), rows("24"), rows("1c"), rows("21"), rows("24"), rows("27"), rows("22"));
    }
}
