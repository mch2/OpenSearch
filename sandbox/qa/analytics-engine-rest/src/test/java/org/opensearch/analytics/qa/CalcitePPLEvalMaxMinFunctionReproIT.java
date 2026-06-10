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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLEvalMaxMinFunctionIT} — the scalar
 * {@code max(...)} / {@code min(...)} eval functions (NOT the stats aggregations) on the
 * analytics-engine route. Uses {@code dog} and {@code null_missing} datasets.
 */
public class CalcitePPLEvalMaxMinFunctionReproIT extends CalciteReproTestCase {

    private static final Dataset DOG = new Dataset("dog", "repro_dog");
    private static final Dataset NM = new Dataset("null_missing", "repro_null_missing");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), DOG);
        DatasetProvisioner.provision(client(), NM);
        provisioned = true;
    }

    private String dog() { return "source=" + DOG.indexName; }
    private String nm() { return "source=" + NM.indexName; }

    public void testEvalMaxNumeric() throws Exception {
        Map<String, Object> r = executePpl(dog() + " | eval new = max(1, 3, age) | fields age, new");
        verifySchema(r, schema("age", "bigint"), schema("new", "int"));
        verifyDataRows(r, rows(2, 3), rows(4, 4));
    }

    public void testEvalMaxString() throws Exception {
        Map<String, Object> r = executePpl(dog()
            + " | eval new = max('apple', 'sam', dog_name) | fields dog_name, new");
        verifySchema(r, schema("dog_name", "string"), schema("new", "string"));
        verifyDataRows(r, rows("rex", "sam"), rows("snoopy", "snoopy"));
    }

    public void testEvalMaxNumericAndString() throws Exception {
        Map<String, Object> r = executePpl(dog()
            + " | eval new = max(14, age, 'Fred', holdersName) | fields age, holdersName, new");
        verifySchema(r, schema("holdersName", "string"), schema("age", "bigint"), schema("new", "string"));
        verifyDataRows(r, rows(2, "Daenerys", "Fred"), rows(4, "Hattie", "Hattie"));
    }

    public void testEvalMinNumeric() throws Exception {
        Map<String, Object> r = executePpl(dog() + " | eval new = min(14, 3, age) | fields age, new");
        verifySchema(r, schema("age", "bigint"), schema("new", "bigint"));
        verifyDataRows(r, rows(2, 2), rows(4, 3));
    }

    public void testEvalMinString() throws Exception {
        Map<String, Object> r = executePpl(dog()
            + " | eval new = min('apple', 'sam', dog_name) | fields dog_name, new");
        verifySchema(r, schema("dog_name", "string"), schema("new", "string"));
        verifyDataRows(r, rows("rex", "apple"), rows("snoopy", "apple"));
    }

    public void testEvalMinNumericAndString() throws Exception {
        Map<String, Object> r = executePpl(dog()
            + " | eval new = min(14, age, 'sam', holdersName) | fields age, holdersName, new");
        verifySchema(r, schema("holdersName", "string"), schema("age", "bigint"), schema("new", "bigint"));
        verifyDataRows(r, rows(2, "Daenerys", 2), rows(4, "Hattie", 4));
    }

    public void testEvalMaxIgnoresNulls() throws Exception {
        Map<String, Object> r = executePpl(nm() + " | eval new = max(`int`, 3) | fields `int`, new");
        verifySchema(r, schema("int", "int"), schema("new", "int"));
        verifyDataRows(r,
            rows(42, 42), rows(null, 3), rows(null, 3), rows(null, 3), rows(null, 3),
            rows(null, 3), rows(null, 3), rows(null, 3), rows(null, 3));
    }

    public void testEvalMinIgnoresNulls() throws Exception {
        Map<String, Object> r = executePpl(nm() + " | eval new = min(dbl, 5) | fields dbl, new");
        verifySchema(r, schema("dbl", "double"), schema("new", "double"));
        verifyDataRows(r,
            rows(3.1415, 3.1415), rows(null, 5), rows(null, 5), rows(null, 5), rows(null, 5),
            rows(null, 5), rows(null, 5), rows(null, 5), rows(null, 5));
    }
}
