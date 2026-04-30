/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for scalar date/time functions routed through PPL → Calcite →
 * Substrait → DataFusion. Bank fixture row 1: created_at='2024-06-15T10:30:00Z'.
 *
 * <p>Zero-arg functions (now, current_date, current_time) are tested by verifying
 * the result is non-null — the exact value depends on wall-clock time.
 */
public class ScalarDateTimeFunctionIT extends BaseScalarFunctionIT {

    // ── Zero-arg time functions ──

    public void testNow() {
        Object cell = evalScalar("now()");
        assertNotNull("now() must not be null", cell);
    }

    public void testCurrentDate() {
        Object cell = evalScalar("current_date()");
        assertNotNull("current_date() must not be null", cell);
    }

    public void testCurrentTime() {
        Object cell = evalScalar("current_time()");
        assertNotNull("current_time() must not be null", cell);
    }

    public void testCurrentTimestamp() {
        Object cell = evalScalar("current_timestamp()");
        assertNotNull("current_timestamp() must not be null", cell);
    }

    public void testSysdate() {
        Object cell = evalScalar("sysdate()");
        assertNotNull("sysdate() must not be null", cell);
    }

    // ── Conversion functions ──

    public void testDate() {
        Object cell = evalScalar("date(created_at)");
        assertNotNull("date(created_at) must not be null", cell);
    }

    public void testUnixTimestamp() {
        Object cell = evalScalar("unix_timestamp(created_at)");
        assertNotNull("unix_timestamp must not be null", cell);
        assertTrue("unix_timestamp must be Number", cell instanceof Number);
    }

    public void testFromUnixtime() {
        Object cell = evalScalar("from_unixtime(1718444400)");
        assertNotNull("from_unixtime must not be null", cell);
    }

    // ── Format functions ──

    public void testDateFormat() {
        Object cell = evalScalar("date_format(created_at, '%Y-%m-%d')");
        assertNotNull("date_format must not be null", cell);
    }

    // ── Make functions ──

    public void testMakeDate() {
        Object cell = evalScalar("makedate(2024, 167)");
        assertNotNull("makedate must not be null", cell);
    }

    public void testMakeTime() {
        Object cell = evalScalar("maketime(10, 30, 0)");
        assertNotNull("maketime must not be null", cell);
    }

    // ── Extract ──

    public void testExtract() {
        Object cell = evalScalar("extract(YEAR FROM created_at)");
        assertNotNull("extract(YEAR) must not be null", cell);
        assertTrue("extract must be Number", cell instanceof Number);
        assertEquals(2024L, ((Number) cell).longValue());
    }
}
