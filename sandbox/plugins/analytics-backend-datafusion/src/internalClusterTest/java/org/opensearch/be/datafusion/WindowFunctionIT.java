/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;

import java.util.List;

/**
 * End-to-end ITs for PPL {@code eventstats} (window aggregates without partitioning,
 * or partitioned by a field). PPL parses {@code eventstats} into a Calcite
 * {@code LogicalProject} with {@code RexOver} expressions; our existing
 * {@code OpenSearchProjectRule} forwards them through isthmus's
 * {@code WindowFunctionConverter}, and DataFusion executes them natively.
 *
 * <p>Bank fixture: 2 rows — Amber/balance=39225 and Hattie/balance=5686.
 *
 * @opensearch.internal
 */
public class WindowFunctionIT extends BaseScalarFunctionIT {

    /** Run {@code source=bank | <pplFragment>} and return the response unfiltered. */
    private PPLResponse runEventstats(String pplFragment) {
        PPLRequest request = new PPLRequest("source=" + BANK_INDEX + " | " + pplFragment);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        return response;
    }

    /**
     * {@code eventstats count() as cnt} over the entire 2-row bank fixture.
     * Every row in the result must carry cnt=2.
     */
    public void testEventstatsCount() {
        PPLResponse response = runEventstats("eventstats count() as cnt");
        List<String> cols = response.getColumns();
        assertTrue("columns must contain cnt, got " + cols, cols.contains("cnt"));
        int idx = cols.indexOf("cnt");
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("cnt must not be null", cell);
            assertEquals("count() over () must equal row count = 2", 2L, ((Number) cell).longValue());
        }
    }

    /**
     * {@code eventstats sum(balance) as s} — total of 39225 + 5686 = 44911 on each row.
     */
    public void testEventstatsSum() {
        PPLResponse response = runEventstats("eventstats sum(balance) as s");
        int idx = response.getColumns().indexOf("s");
        assertTrue("columns must contain s, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("s must not be null", cell);
            assertEquals("sum(balance) over () = 39225 + 5686", 44911L, ((Number) cell).longValue());
        }
    }

    /**
     * {@code eventstats avg(balance) as a} — (39225 + 5686) / 2 = 22455.5 on each row.
     */
    public void testEventstatsAvg() {
        PPLResponse response = runEventstats("eventstats avg(balance) as a");
        int idx = response.getColumns().indexOf("a");
        assertTrue("columns must contain a, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("a must not be null", cell);
            assertEquals("avg(balance) over ()", 22455.5d, ((Number) cell).doubleValue(), 1e-9);
        }
    }

    /**
     * {@code eventstats min(balance) as m} — min of {39225, 5686} = 5686 on each row.
     */
    public void testEventstatsMin() {
        PPLResponse response = runEventstats("eventstats min(balance) as m");
        int idx = response.getColumns().indexOf("m");
        assertTrue("columns must contain m, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("m must not be null", cell);
            assertEquals("min(balance) over ()", 5686L, ((Number) cell).longValue());
        }
    }

    /**
     * {@code eventstats max(balance) as m} — max of {39225, 5686} = 39225 on each row.
     */
    public void testEventstatsMax() {
        PPLResponse response = runEventstats("eventstats max(balance) as m");
        int idx = response.getColumns().indexOf("m");
        assertTrue("columns must contain m, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("m must not be null", cell);
            assertEquals("max(balance) over ()", 39225L, ((Number) cell).longValue());
        }
    }

    /**
     * {@code eventstats count() as cnt by firstname} — each firstname appears once
     * in the bank fixture, so the per-partition count must be 1 on every row.
     */
    public void testEventstatsCountByFirstname() {
        PPLResponse response = runEventstats("eventstats count() as cnt by firstname");
        int idx = response.getColumns().indexOf("cnt");
        assertTrue("columns must contain cnt, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("cnt must not be null", cell);
            assertEquals("count() over (partition by firstname) — unique names → 1", 1L, ((Number) cell).longValue());
        }
    }

    /**
     * {@code eventstats stddev_pop(balance) as s} — population stddev of {39225, 5686}.
     * mean = 22455.5; deviations = ±16769.5; variance = 16769.5^2; stddev = 16769.5.
     */
    public void testEventstatsStddevPop() {
        PPLResponse response = runEventstats("eventstats stddev_pop(balance) as s");
        int idx = response.getColumns().indexOf("s");
        assertTrue("columns must contain s, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("s must not be null", cell);
            assertEquals("stddev_pop(balance) over ()", 16769.5d, ((Number) cell).doubleValue(), 1e-6);
        }
    }

    /**
     * {@code eventstats var_pop(balance) as v} — population variance of {39225, 5686}.
     * mean = 22455.5; variance = 16769.5^2 = 281216130.25.
     */
    public void testEventstatsVarPop() {
        PPLResponse response = runEventstats("eventstats var_pop(balance) as v");
        int idx = response.getColumns().indexOf("v");
        assertTrue("columns must contain v, got " + response.getColumns(), idx >= 0);
        assertEquals("eventstats must preserve all 2 rows", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("v must not be null", cell);
            assertEquals("var_pop(balance) over ()", 281216130.25d, ((Number) cell).doubleValue(), 1e-3);
        }
    }
}
