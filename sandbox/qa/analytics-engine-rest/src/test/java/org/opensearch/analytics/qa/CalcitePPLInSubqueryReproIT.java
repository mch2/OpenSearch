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
 * Reproduction of representative failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLInSubqueryIT} on the analytics-engine route.
 * Uses {@code worker} (+ doc7 Tommy), {@code work_information}, {@code occupation}.
 */
public class CalcitePPLInSubqueryReproIT extends CalciteReproTestCase {

    private static final Dataset WORKER = new Dataset("worker", "repro_worker");
    private static final Dataset WORKINFO = new Dataset("work_information", "repro_work_information");
    private static final Dataset OCC = new Dataset("occupation", "repro_occupation");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), WORKER);
        DatasetProvisioner.provision(client(), WORKINFO);
        DatasetProvisioner.provision(client(), OCC);
        // Upstream init() adds worker doc 7 (Tommy).
        indexDoc(WORKER.indexName, "7",
            "{\"id\":1006,\"name\":\"Tommy\",\"occupation\":\"Teacher\",\"country\":\"USA\",\"salary\":30000}");
        provisioned = true;
    }

    public void testWhereInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id in [ source = " + WORKINFO.indexName + " | fields uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testFilterInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id in [ source = " + WORKINFO.indexName + " | fields uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testInSubqueryWithTableAlias() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName + " as o"
            + " | where id in [ source = " + WORKINFO.indexName + " as i | where i.department = 'DATA' | fields uid ]"
            + " | sort - o.salary | fields o.id, o.name, o.salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result, rows(1002, "John", 120000), rows(1005, "Jane", 90000));
    }

    public void testTwoExpressionsInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where (id, name) in [ source = " + WORKINFO.indexName + " | fields uid, name ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000));
    }

    public void testInCorrelatedSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where name in [ source = " + WORKINFO.indexName
            + " | where id = uid and (like(occupation, '%ist') or occupation = 'Engineer') | fields name ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1000, "Jake", 100000), rows(1005, "Jane", 90000));
    }

    public void testEmptyInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id not in [ source = " + WORKINFO.indexName + " | where uid = 0000 | fields uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1001, "Hello", 70000), rows(1006, "Tommy", 30000),
            rows(1004, "David", 0));
    }

    public void testInSubqueryWithParentheses() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where (id) in [ source = " + WORKINFO.indexName + " | fields uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1000, "Jake", 100000),
            rows(1005, "Jane", 90000), rows(1006, "Tommy", 30000));
    }

    public void testNestedInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id in [ source = " + WORKINFO.indexName
            + " | where occupation in [ source = " + OCC.indexName
            + " | where occupation != 'Engineer' | fields occupation ] | fields uid ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1002, "John", 120000), rows(1003, "David", 120000), rows(1006, "Tommy", 30000));
    }

    public void testNestedInSubquery2() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id in [ source = " + WORKINFO.indexName
            + " | where occupation in [ source = " + OCC.indexName
            + " | where occupation != 'Engineer' | fields occupation ] | fields uid ]"
            + " | sort - salary | fields name, country, occupation, id, salary");
        verifySchema(result,
            schema("name", "string"), schema("country", "string"), schema("occupation", "string"),
            schema("id", "int"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows("John", "Canada", "Doctor", 1002, 120000),
            rows("David", null, "Doctor", 1003, 120000),
            rows("Tommy", "USA", "Teacher", 1006, 30000));
    }

    public void testSelfInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where id in [ source=" + WORKER.indexName + " | where country = 'USA' | fields id ]"
            + " | fields name, country, occupation, id, salary");
        verifySchema(result,
            schema("name", "string"), schema("country", "string"), schema("occupation", "string"),
            schema("id", "int"), schema("salary", "int"));
        verifyDataRows(result,
            rows("Hello", "USA", "Artist", 1001, 70000),
            rows("Tommy", "USA", "Teacher", 1006, 30000));
    }

    public void testTwoExpressionsNotInSubquery() throws IOException {
        Map<String, Object> result = executePpl("source = " + WORKER.indexName
            + " | where (id, name) not in [ source = " + WORKINFO.indexName + " | fields uid, name ]"
            + " | sort - salary | fields id, name, salary");
        verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
        verifyDataRowsInOrder(result,
            rows(1001, "Hello", 70000), rows(1006, "Tommy", 30000), rows(1004, "David", 0));
    }

    public void testSubsearchMaxOut() throws IOException {
        setSubsearchMaxOut(1);
        try {
            Map<String, Object> result = executePpl("source = " + WORKER.indexName
                + " | where id in [ source = " + WORKINFO.indexName + " | fields uid ]"
                + " | sort - salary | fields id, name, salary");
            verifySchema(result, schema("id", "int"), schema("name", "string"), schema("salary", "int"));
            verifyDataRowsInOrder(result, rows(1000, "Jake", 100000));
        } finally {
            setSubsearchMaxOut(10000);
        }
    }

    public void testSubsearchMaxOutZeroMeansUnlimited() throws IOException {
        setSubsearchMaxOut(0);
        try {
            Map<String, Object> result = executePpl("source = " + WORKER.indexName
                + " | where id in [ source = " + WORKINFO.indexName + " | fields uid ]"
                + " | sort - salary | fields id, name, salary");
            verifyNumOfRows(result, 5);
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
