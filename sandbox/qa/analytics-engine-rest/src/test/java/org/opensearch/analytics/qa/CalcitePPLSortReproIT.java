/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reproduction of failing methods from upstream
 * {@code org.opensearch.sql.calcite.remote.CalcitePPLSortIT} on the analytics-engine route.
 * Uses {@code bank} and {@code bank_null} datasets. All use explicit {@code | fields}, so column
 * order (bucket A) does not apply — these probe sort ordering / null placement / AUTO cast.
 */
public class CalcitePPLSortReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_sort_bank");
    private static final Dataset BANK_NULL = new Dataset("bank_null", "repro_sort_bank_null");
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        DatasetProvisioner.provision(client(), BANK_NULL);
        provisioned = true;
    }

    public void testSortAgeAndFieldsNameAge() throws IOException {
        Map<String, Object> actual = executePpl("source=" + BANK.indexName
            + " | sort - age | fields firstname, age");
        verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
        verifyDataRowsInOrder(actual,
            rows("Virginia", 39), rows("Hattie", 36), rows("Elinor", 36), rows("Dillard", 34),
            rows("Dale", 33), rows("Amber JOHnny", 32), rows("Nanette", 28));
    }

    public void testSortWithAutoCast() throws IOException {
        Map<String, Object> result = executePpl("source=" + BANK.indexName
            + " | sort AUTO(age) | fields firstname, age");
        verifySchema(result, schema("firstname", "string"), schema("age", "int"));
        verifyDataRowsInOrder(result,
            rows("Nanette", 28), rows("Amber JOHnny", 32), rows("Dale", 33), rows("Dillard", 34),
            rows("Hattie", 36), rows("Elinor", 36), rows("Virginia", 39));
    }

    public void testSortWithNullValue() throws IOException {
        // Nulls sort first (ascending). The 3 null-balance rows may come in any relative order;
        // upstream asserts the null set then the non-null order. We assert: first 3 rows are the
        // null-balance set {Hattie, Elinor, Virginia}, remaining 4 in ascending balance order.
        Map<String, Object> result = executePpl("source=" + BANK_NULL.indexName
            + " | sort balance | fields firstname, balance");
        List<List<Object>> rows = dataRowsOf(result);
        assertEquals("row count", 7, rows.size());
        java.util.Set<Object> nullNames = new java.util.HashSet<>();
        for (int i = 0; i < 3; i++) {
            assertNull("expected null balance in first 3 rows but was " + rows.get(i), rows.get(i).get(1));
            nullNames.add(rows.get(i).get(0));
        }
        assertEquals(java.util.Set.of("Hattie", "Elinor", "Virginia"), nullNames);
        verifyTail(rows, 3,
            rows("Dale", 4180), rows("Nanette", 32838), rows("Amber JOHnny", 39225), rows("Dillard", 48086));
    }

    @SafeVarargs
    private final void verifyTail(List<List<Object>> rows, int from, List<Object>... expected) {
        for (int i = 0; i < expected.length; i++) {
            assertEquals("tail row " + (from + i), expected[i], rows.get(from + i));
        }
    }
}
