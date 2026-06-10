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
 * Reproduction of assorted single-method failures that reuse already-provisioned datasets:
 * <ul>
 *   <li>{@code CalcitePPLRenameIT#testRenameFullWildcardExcludesMetadataFields} (state_country)</li>
 *   <li>{@code CalciteSystemFunctionIT#typeof_opensearch_types} (datatypes_numeric — numeric half only;
 *       the non-numeric half needs geo_point which AE rejects, see bucket E)</li>
 *   <li>{@code CalciteSettingsIT#testQuerySizeLimit} (bank)</li>
 * </ul>
 */
public class CalciteMiscReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country", "repro_misc_sc");
    private static final Dataset SCN = new Dataset("state_country_null", "repro_misc_scn");
    private static final Dataset NUM = new Dataset("datatypes_numeric", "repro_misc_numeric");
    private static final Dataset BANK = new Dataset("bank", "repro_misc_bank");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        DatasetProvisioner.provision(client(), SCN);
        DatasetProvisioner.provision(client(), NUM);
        DatasetProvisioner.provision(client(), BANK);
        provisioned = true;
    }

    /** CalcitePPLCaseFunctionIT#testCaseAggWithNullValues — range-case agg drops the null bucket. */
    public void testCaseAggWithNullValues() throws IOException {
        Map<String, Object> actual = executePpl("source=" + SCN.indexName
            + " | eval age_category = case(age < 20, 'teenager', age < 70, 'adult', age >= 70, 'senior'"
            + " else 'unknown') | stats avg(age) by age_category");
        verifySchema(actual, schema("avg(age)", "double"), schema("age_category", "string"));
        verifyDataRows(actual, rows(10, "teenager"), rows(25, "adult"), rows(70, "senior"));
    }

    public void testRenameFullWildcardExcludesMetadataFields() throws IOException {
        Map<String, Object> result = executePpl("source = " + SC.indexName + " | rename * as old_*");
        verifySchema(result,
            schema("old_name", "string"), schema("old_age", "int"), schema("old_state", "string"),
            schema("old_country", "string"), schema("old_year", "int"), schema("old_month", "int"));
        verifyDataRows(result,
            rows("Jake", "USA", "California", 4, 2023, 70),
            rows("Hello", "USA", "New York", 4, 2023, 30),
            rows("John", "Canada", "Ontario", 4, 2023, 25),
            rows("Jane", "Canada", "Quebec", 4, 2023, 20));
    }

    public void typeof_opensearch_types() throws IOException {
        Map<String, Object> response = executePpl("source=" + NUM.indexName
            + " | eval `double` = typeof(double_number), `long` = typeof(long_number),"
            + " `integer` = typeof(integer_number), `byte` = typeof(byte_number),"
            + " `short` = typeof(short_number), `float` = typeof(float_number),"
            + " `half_float` = typeof(half_float_number), `scaled_float` = typeof(scaled_float_number)"
            + " | fields `double`, `long`, `integer`, `byte`, `short`, `float`, `half_float`, `scaled_float`");
        verifyDataRows(response,
            rows("DOUBLE", "BIGINT", "INT", "TINYINT", "SMALLINT", "FLOAT", "FLOAT", "DOUBLE"));
    }

    public void testQuerySizeLimit() throws IOException {
        Map<String, Object> result = executePpl("search source=" + BANK.indexName
            + " age>35 | fields firstname");
        verifyDataRows(result, rows("Hattie"), rows("Elinor"), rows("Virginia"));

        setQuerySizeLimit(1);
        try {
            result = executePpl("search source=" + BANK.indexName + " age>35 | fields firstname");
            verifyDataRows(result, rows("Hattie"));
        } finally {
            setQuerySizeLimit(null);
        }
    }

    private void setQuerySizeLimit(Integer n) throws IOException {
        org.opensearch.client.Request req =
            new org.opensearch.client.Request("PUT", "/_cluster/settings");
        String v = n == null ? "null" : String.valueOf(n);
        req.setJsonEntity("{\"transient\":{\"plugins.query.size_limit\":" + v + "}}");
        client().performRequest(req);
    }
}
