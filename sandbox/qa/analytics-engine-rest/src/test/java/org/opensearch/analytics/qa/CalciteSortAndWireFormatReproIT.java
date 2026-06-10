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
 * Reproduction of remaining sort tests ({@code CalciteSortCommandIT}) and the analytics-engine
 * datetime wire-format tests ({@code CalciteAnalyticsDatetimeWireFormatIT}) that fail on the
 * analytics-engine route. Uses {@code bank} and an inline {@code wire_format_dt} index.
 */
public class CalciteSortAndWireFormatReproIT extends CalciteReproTestCase {

    private static final Dataset BANK = new Dataset("bank", "repro_sortwf_bank");
    private static final String WF = "repro_wire_format_dt";
    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        DatasetProvisioner.provision(client(), BANK);
        // wire_format_dt: date/date_nanos/date-only/time-only mapped columns.
        createParquetIndex(WF,
            "{\"ts\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd HH:mm:ss\"},"
                + "\"ts_nanos\":{\"type\":\"date_nanos\"},"
                + "\"d\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd\"},"
                + "\"t\":{\"type\":\"date\",\"format\":\"HH:mm:ss\"}}");
        indexDoc(WF, "1",
            "{\"ts\":\"2024-03-15 10:30:00\",\"ts_nanos\":\"2024-03-15T10:30:00.123456789Z\","
                + "\"d\":\"2024-03-15\",\"t\":\"10:30:00\"}");
        indexDoc(WF, "2",
            "{\"ts\":\"2024-03-16 23:59:59\",\"ts_nanos\":\"2024-03-16T23:59:59.999999999Z\","
                + "\"d\":\"2024-03-16\",\"t\":\"23:59:59\"}");
        provisioned = true;
    }

    // ── CalciteSortCommandIT ──────────────────────────────────────────────────

    public void testHeadThenSort() throws IOException {
        Map<String, Object> result = executePpl("source=" + BANK.indexName
            + " | head 2 | sort age | fields age");
        // pushdown enabled on AE: head 2 takes the first two docs, sort orders them.
        verifyDataRowsInOrder(result, rows(28), rows(32));
    }

    public void testPushdownSortCastToDoubleExpression() throws IOException {
        // The upstream test also asserts on DSL explain output (engine-internal); here we pin the
        // user-visible result: eval cast→double, sort by it, first two rows.
        Map<String, Object> result = executePpl("source=" + BANK.indexName
            + " | eval age2 = cast(age as double) | sort age2 | fields age, age2 | head 2");
        verifySchema(result, schema("age", "int"), schema("age2", "double"));
        verifyDataRowsInOrder(result, rows(28, 28d), rows(32, 32d));
    }

    // ── CalciteAnalyticsDatetimeWireFormatIT ──────────────────────────────────

    public void testDateRootColumnYmdFormat() throws IOException {
        Map<String, Object> result = executePpl("source=" + WF + " | where d = '2024-03-15' | fields d");
        verifySchema(result, schema("d", "timestamp"));
        verifyDataRows(result, rows("2024-03-15 00:00:00"));
    }

    public void testTimeRootColumnHmsFormat() throws IOException {
        Map<String, Object> result = executePpl("source=" + WF + " | sort t | head 1 | fields t");
        verifySchema(result, schema("t", "timestamp"));
        List<List<Object>> rows = dataRowsOf(result);
        assertFalse("time-mapped column must not surface as ISO T-separator literal",
            String.valueOf(rows.get(0).get(0)).contains("T"));
    }
}
