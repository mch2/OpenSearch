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
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLExistsSubqueryIT} on the analytics-engine
 * route. Uses {@code worker} (+ doc7 Tommy), {@code work_information}, {@code occupation}.
 */
public class CalcitePPLExistsSubqueryReproIT extends CalciteReproTestCase {

    private static final Dataset WORKER = new Dataset("worker", "repro_ex_worker");
    private static final Dataset WORKINFO = new Dataset("work_information", "repro_ex_work_information");
    private static final Dataset OCC = new Dataset("occupation", "repro_ex_occupation");
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
    private String occ() { return OCC.indexName; }

    public void testSimpleExistsSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " | where id = uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testSimpleExistsSubqueryInFilter() throws IOException {
        Map<String, Object> result = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " | where id = uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testUncorrelatedExistsSubquery() throws IOException {
        Map<String, Object> r1 = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " | where name = 'Tom' ]"
            + " | sort - salary | fields id, name, salary");
        verifyNumOfRows(r1, 7);

        Map<String, Object> r2 = executePpl("source = " + w()
            + " | where not exists [ source = " + wi() + " | where name = 'Tom' ]"
            + " | sort - salary | fields id, name, salary");
        verifyNumOfRows(r2, 0);
    }

    public void testNestedExistsSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + w()
            + " | where exists [ source = " + wi()
            + " | where exists [ source = " + occ()
            + " | where " + occ() + ".occupation = " + wi() + ".occupation ] | where id = uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testExistsSubqueryAndAggregation() throws IOException {
        Map<String, Object> result = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " | where id = uid ] | stats count() by country");
        verifySchema(result, schema("country", "string"), schema("count()", "bigint"));
        verifyDataRows(result, rows(1, null), rows(2, "Canada"), rows(1, "USA"), rows(1, "England"));
    }

    public void testIssue3566() throws IOException {
        Map<String, Object> result = executePpl("source = " + w()
            + " | fields id, country | where exists [ source = " + wi() + " | where id = uid ]"
            + " | stats count() by country");
        verifySchemaInOrder(result, schema("count()", "bigint"), schema("country", "string"));
        verifyDataRows(result, rows(1, null), rows(1, "England"), rows(1, "USA"), rows(2, "Canada"));
    }

    public void testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot() throws IOException {
        Map<String, Object> r1 = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " ] | eval constant = \"Bala\" | fields constant");
        verifyDataRows(r1,
            rows("Bala"), rows("Bala"), rows("Bala"), rows("Bala"), rows("Bala"), rows("Bala"), rows("Bala"));

        Map<String, Object> r2 = executePpl("source = " + w()
            + " | where exists [ source = " + wi() + " | where uid = 999 ] | eval constant = 'Bala' | fields constant");
        verifyNumOfRows(r2, 0);
    }

    // ── subsearch maxout ───────────────────────────────────────────────────────

    public void testSubsearchMaxOut1() throws IOException {
        withMaxOut(1, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi() + " | where id = uid ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 1);
        });
    }

    public void testSubsearchMaxOut2() throws IOException {
        withMaxOut(2, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi() + " | where id = uid and department = 'DATA' ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 2);
        });
    }

    public void testSubsearchMaxOut3() throws IOException {
        withMaxOut(2, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi()
                + " | where id = uid | eval dept = department | where dept = 'DATA' | sort - dept ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 1);
        });
    }

    public void testSubsearchMaxOut4() throws IOException {
        withMaxOut(2, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi()
                + " | eval dept = department | where dept = 'DATA' | where id = uid ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 2);
        });
    }

    public void testSubsearchMaxOutNegativeMeansUnlimited() throws IOException {
        withMaxOut(-1, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi() + " | where id = uid ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 5);
        });
    }

    public void testSubsearchMaxOutUncorrelated() throws IOException {
        withMaxOut(1, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi() + " | join type=left uid " + wi()
                + " | eval dept = department | where dept = 'DATA' ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 7);
        });
    }

    public void testUncorrelatedSubsearchMaxOutZeroMeansUnlimited() throws IOException {
        withMaxOut(0, () -> {
            Map<String, Object> r = executePpl("source = " + w()
                + " | where exists [ source = " + wi() + " | where name = 'Tom' ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(r, 7);
        });
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private interface IOAction { void run() throws IOException; }

    private void withMaxOut(int n, IOAction action) throws IOException {
        setSubsearchMaxOut(n);
        try {
            action.run();
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
