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
}
