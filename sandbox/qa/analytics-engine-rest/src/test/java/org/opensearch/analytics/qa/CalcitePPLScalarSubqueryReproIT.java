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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLScalarSubqueryIT} on the analytics-engine
 * route. Uses {@code worker} (+ doc7 Tommy), {@code work_information}, {@code occupation}.
 */
public class CalcitePPLScalarSubqueryReproIT extends CalciteReproTestCase {

    private static final Dataset WORKER = new Dataset("worker", "repro_ss_worker");
    private static final Dataset WORKINFO = new Dataset("work_information", "repro_ss_work_information");
    private static final Dataset OCC = new Dataset("occupation", "repro_ss_occupation");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), WORKER);
        DatasetProvisioner.provision(client(), WORKINFO);
        DatasetProvisioner.provision(client(), OCC);
        indexDoc(WORKER.indexName, "7",
            "{\"id\":1006,\"name\":\"Tommy\",\"occupation\":\"Teacher\",\"country\":\"USA\",\"salary\":30000}");
        provisioned = true;
    }

    private String w() { return WORKER.indexName; }
    private String wi() { return WORKINFO.indexName; }

    public void testUncorrelatedScalarSubqueryInSelect() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | eval count_dept = [ source = " + wi() + " | stats count(department) ]"
            + " | fields name, count_dept");
        verifySchema(r, schema("name", "string"), schema("count_dept", "bigint"));
        verifyDataRows(r,
            rows("Jake", 5), rows("Hello", 5), rows("John", 5), rows("David", 5),
            rows("David", 5), rows("Jane", 5), rows("Tommy", 5));
    }

    public void testUncorrelatedScalarSubqueryInSelectAndWhere() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id > [ source = " + wi() + " | stats count(department) ] + 999"
            + " | eval count_dept = [ source = " + wi() + " | stats count(department) ]"
            + " | fields name, count_dept");
        verifySchema(r, schema("name", "string"), schema("count_dept", "bigint"));
        verifyDataRows(r, rows("Jane", 5), rows("Tommy", 5));
    }

    public void testUncorrelatedScalarSubqueryInSelectAndInFilter() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id > [ source = " + wi() + " | stats count(department) ] + 999"
            + " | eval count_dept = [ source = " + wi() + " | stats count(department) ]"
            + " | fields name, count_dept");
        verifySchema(r, schema("name", "string"), schema("count_dept", "bigint"));
        verifyDataRows(r, rows("Jane", 5), rows("Tommy", 5));
    }

    public void testCorrelatedScalarSubqueryInSelect() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | eval count_dept = [ source = " + wi() + " | where id = uid | stats count(department) ]"
            + " | fields id, name, count_dept");
        verifySchema(r, schema("id", "int"), schema("name", "string"), schema("count_dept", "bigint"));
        verifyDataRows(r,
            rows(1000, "Jake", 1), rows(1001, "Hello", 0), rows(1002, "John", 1), rows(1003, "David", 1),
            rows(1004, "David", 0), rows(1005, "Jane", 1), rows(1006, "Tommy", 1));
    }

    public void testCorrelatedScalarSubqueryInSelectWithNonEqual() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | eval count_dept = [ source = " + wi() + " | where id > uid | stats count(department) ]"
            + " | fields id, name, count_dept");
        verifySchema(r, schema("id", "int"), schema("name", "string"), schema("count_dept", "bigint"));
        verifyDataRows(r,
            rows(1000, "Jake", 0), rows(1001, "Hello", 1), rows(1002, "John", 1), rows(1003, "David", 2),
            rows(1004, "David", 3), rows(1005, "Jane", 3), rows(1006, "Tommy", 4));
    }

    public void testCorrelatedScalarSubqueryInWhere() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id = [ source = " + wi() + " | where id = uid | stats max(uid) ]"
            + " | fields id, name");
        verifySchema(r, schema("id", "int"), schema("name", "string"));
        verifyDataRows(r,
            rows(1000, "Jake"), rows(1002, "John"), rows(1003, "David"), rows(1005, "Jane"), rows(1006, "Tommy"));
    }

    public void testCorrelatedScalarSubqueryInFilter() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id = [ source = " + wi() + " | where id = uid | stats max(uid) ]"
            + " | fields id, name");
        verifySchema(r, schema("id", "int"), schema("name", "string"));
        verifyDataRows(r,
            rows(1000, "Jake"), rows(1002, "John"), rows(1003, "David"), rows(1005, "Jane"), rows(1006, "Tommy"));
    }

    public void testTwoUncorrelatedScalarSubqueriesInOr() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id = [ source = " + wi() + " | sort uid | stats max(uid) ]"
            + " OR id = [ source = " + wi() + " | sort uid | where department = 'DATA' | stats min(uid) ]"
            + " | fields id, name");
        verifySchema(r, schema("id", "int"), schema("name", "string"));
        verifyDataRows(r, rows(1002, "John"), rows(1006, "Tommy"));
    }

    public void testTwoCorrelatedScalarSubqueriesInOr() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where id = [ source = " + wi() + " | where id = uid | stats max(uid) ]"
            + " OR id = [ source = " + wi() + " | sort uid | where department = 'DATA' | stats min(uid) ]"
            + " | fields id, name");
        verifySchema(r, schema("id", "int"), schema("name", "string"));
        verifyDataRows(r,
            rows(1000, "Jake"), rows(1002, "John"), rows(1003, "David"), rows(1005, "Jane"), rows(1006, "Tommy"));
    }

    public void testDisjunctiveCorrelatedScalarSubquery() throws IOException {
        Map<String, Object> r = executePpl("source = " + w()
            + " | where [ source = " + wi() + " | where id = uid OR uid = 1010 | stats count() ] > 0"
            + " | fields id, name");
        verifySchema(r, schema("id", "int"), schema("name", "string"));
        verifyDataRows(r,
            rows(1000, "Jake"), rows(1002, "John"), rows(1003, "David"), rows(1005, "Jane"), rows(1006, "Tommy"));
    }

    public void testSubsearchMaxOutZeroMeansUnlimited() throws IOException {
        setSubsearchMaxOut(0);
        try {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where id = [ source = " + wi() + " | where id = uid | stats max(uid) ]"
                + " | fields id, name");
            verifyNumOfRows(r, 5);
        } finally {
            setSubsearchMaxOut(10000);
        }
    }

    private void setSubsearchMaxOut(int n) throws IOException {
        org.opensearch.client.Request req =
            new org.opensearch.client.Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"plugins.ppl.subsearch.maxout\":" + n + "}}");
        client().performRequest(req);
    }
}
