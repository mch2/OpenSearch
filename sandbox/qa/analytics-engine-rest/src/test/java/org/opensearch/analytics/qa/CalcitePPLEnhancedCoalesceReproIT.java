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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLEnhancedCoalesceIT} on the analytics-engine
 * route. Uses {@code state_country_null} plus the two extra docs (id9 score/active, id10 empty
 * name/null age) the upstream test injects in {@code init()}.
 */
public class CalcitePPLEnhancedCoalesceReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country_null", "repro_coalesce_sc");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        indexDoc(SC.indexName, "9",
            "{\"name\":null,\"age\":25,\"score\":85.5,\"active\":true,\"year\":2023,\"month\":4}");
        indexDoc(SC.indexName, "10",
            "{\"name\":\"\",\"age\":null,\"score\":null,\"active\":false,\"year\":2023,\"month\":4}");
        provisioned = true;
    }

    private String src() { return "source=" + SC.indexName; }

    public void testCoalesceWithNonExistentField() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(nonexistent_field, name) | fields name, result | head 2");
        verifySchema(actual, schema("name", "string"), schema("result", "string"));
        verifyDataRows(actual, rows("Jake", "Jake"), rows("Hello", "Hello"));
    }

    public void testCoalesceWithMultipleNonExistentFields() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(field1, field2, name, 'fallback') | fields name, result | head 1");
        verifySchema(actual, schema("name", "string"), schema("result", "string"));
        verifyDataRows(actual, rows("Jake", "Jake"));
    }

    public void testCoalesceWithAllNonExistentFields() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(field1, field2, field3) | fields name, result | head 1");
        verifySchema(actual, schema("name", "string"), schema("result", "undefined"));
        verifyDataRows(actual, rows("Jake", null));
    }

    public void testCoalesceWithNullLiteralAndIntegerField() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(null, age) | fields age, result | head 3");
        verifySchema(actual, schema("age", "int"), schema("result", "int"));
        verifyDataRows(actual, rows(70, 70), rows(30, 30), rows(25, 25));
    }

    public void testCoalesceWithSpaceString() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(' ', name) | fields name, result | head 1");
        verifySchema(actual, schema("name", "string"), schema("result", "string"));
        verifyDataRows(actual, rows("Jake", " "));
    }

    public void testCoalesceWithCompatibleNumericTypes() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(age, year, 999) | fields age, year, result | head 2");
        verifySchema(actual, schema("age", "int"), schema("year", "int"), schema("result", "int"));
        verifyDataRows(actual, rows(70, 2023, 70), rows(30, 2023, 30));
    }

    public void testCoalesceWithCompatibleNumericAndTemporalTypes() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval result = coalesce(age, year, month) | fields age, year, month, result | head 2");
        verifySchema(actual,
            schema("age", "int"), schema("year", "int"), schema("month", "int"), schema("result", "int"));
        verifyDataRows(actual, rows(70, 2023, 4, 70), rows(30, 2023, 4, 30));
    }
}
