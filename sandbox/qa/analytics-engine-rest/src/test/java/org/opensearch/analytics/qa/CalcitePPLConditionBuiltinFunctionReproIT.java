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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLConditionBuiltinFunctionIT} on the
 * analytics-engine route. Uses the {@code state_country_null} dataset plus two extra docs
 * (whitespace-name id7, empty-name id8) the upstream test injects in {@code init()}.
 */
public class CalcitePPLConditionBuiltinFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset SC = new Dataset("state_country_null", "repro_state_country_null");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), SC);
        // Upstream injects these two extra docs in init().
        indexDoc(SC.indexName, "7",
            "{\"name\":\"    \",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
        indexDoc(SC.indexName, "8",
            "{\"name\":\"\",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
        provisioned = true;
    }

    private String src() { return "source=" + SC.indexName; }

    public void testIsBlank() throws IOException {
        Map<String, Object> actual = executePpl(src() + " | where isblank(name) | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "int"));
        verifyDataRows(actual, rows(null, 10), rows("    ", 27), rows("", 57));
    }

    public void testIsPresent() throws IOException {
        Map<String, Object> actual = executePpl(src() + " | where ispresent(name) | fields name, age");
        verifySchema(actual, schema("name", "string"), schema("age", "int"));
        verifyDataRows(actual,
            rows("Jake", 70), rows("Hello", 30), rows("John", 25), rows("Jane", 20),
            rows("Kevin", null), rows("    ", 27), rows("", 57));
    }

    public void testIsNotNull() throws IOException {
        Map<String, Object> actual = executePpl(src() + " | where isnotnull(name) | fields name");
        verifySchema(actual, schema("name", "string"));
        verifyDataRows(actual,
            rows("John"), rows("Jane"), rows("Jake"), rows("Hello"), rows("Kevin"), rows("    "), rows(""));
    }

    public void testIf() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | where isnotnull(age) | eval judge = if(age>50, 'old', 'young') | fields judge, age");
        verifySchema(actual, schema("judge", "string"), schema("age", "int"));
        verifyDataRows(actual,
            rows("young", 25), rows("young", 20), rows("young", 10), rows("old", 70),
            rows("young", 30), rows("young", 27), rows("old", 57));
    }

    public void testNullIf() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval new_age = nullif(age, 20) | fields name, new_age");
        verifySchema(actual, schema("name", "string"), schema("new_age", "int"));
        verifyDataRows(actual,
            rows("John", 25), rows("Jane", null), rows(null, 10), rows("Jake", 70),
            rows("Kevin", null), rows("Hello", 30), rows("    ", 27), rows("", 57));
    }

    public void testNullIfWithExpression() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval new_age = nullif(age + 0, 20) | fields name, new_age");
        verifySchema(actual, schema("name", "string"), schema("new_age", "int"));
        verifyDataRows(actual,
            rows("John", 25), rows("Jane", null), rows(null, 10), rows("Jake", 70),
            rows("Kevin", null), rows("Hello", 30), rows("    ", 27), rows("", 57));
    }

    public void testIfNull() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval new_name = ifnull(name, 'Unknown') | fields new_name, age");
        verifySchema(actual, schema("new_name", "string"), schema("age", "int"));
        verifyDataRows(actual,
            rows("John", 25), rows("Jane", 20), rows("Unknown", 10), rows("Jake", 70),
            rows("Kevin", null), rows("Hello", 30), rows("    ", 27), rows("", 57));
    }

    public void testEvalIsNotNullDirect() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval is_not_null_name=isnotnull(name) | fields name, is_not_null_name");
        verifySchema(actual, schema("name", "string"), schema("is_not_null_name", "boolean"));
        verifyDataRows(actual,
            rows("John", true), rows("Jane", true), rows(null, false), rows("Jake", true),
            rows("Kevin", true), rows("Hello", true), rows("    ", true), rows("", true));
    }

    public void testIsNotNullWithSingleNotEquals() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | where name != 'Jake' and isnotnull(name) | fields name");
        verifySchema(actual, schema("name", "string"));
        verifyDataRows(actual,
            rows("John"), rows("Jane"), rows("Hello"), rows("Kevin"), rows("    "), rows(""));
    }

    public void testIsNotNullWithMultipleNotEquals() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | where name != 'Jake' and name != 'Hello' and isnotnull(name) | fields name");
        verifySchema(actual, schema("name", "string"));
        verifyDataRows(actual, rows("John"), rows("Jane"), rows("Kevin"), rows("    "), rows(""));
    }

    public void testEvalIsNullWithIf() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval n=if(isnull(name), 'yes', 'no') | fields name, n");
        verifySchema(actual, schema("name", "string"), schema("n", "string"));
        verifyDataRows(actual,
            rows("John", "no"), rows("Jane", "no"), rows(null, "yes"), rows("Jake", "no"),
            rows("Kevin", "no"), rows("Hello", "no"), rows("    ", "no"), rows("", "no"));
    }

    public void testEvalIsNullInComplexExpression() throws IOException {
        Map<String, Object> actual = executePpl(src()
            + " | eval safe_name=if(isnull(name), 'Unknown', name) | fields safe_name, age");
        verifySchema(actual, schema("safe_name", "string"), schema("age", "int"));
        verifyDataRows(actual,
            rows("John", 25), rows("Jane", 20), rows("Unknown", 10), rows("Jake", 70),
            rows("Kevin", null), rows("Hello", 30), rows("    ", 27), rows("", 57));
    }
}
