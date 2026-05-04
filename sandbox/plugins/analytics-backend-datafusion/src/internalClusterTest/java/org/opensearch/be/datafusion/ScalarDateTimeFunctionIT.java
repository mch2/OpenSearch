/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.junit.Ignore;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;

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

    // ── date_part-based functions (rewritten by DatePartAdapter) ──

    public void testYear() {
        Object cell = evalScalar("year(created_at)");
        assertNotNull("year() must not be null", cell);
        assertEquals(2024L, ((Number) cell).longValue());
    }

    public void testMonth() {
        Object cell = evalScalar("month(created_at)");
        assertNotNull("month() must not be null", cell);
        assertEquals(6L, ((Number) cell).longValue());
    }

    public void testDay() {
        Object cell = evalScalar("day(created_at)");
        assertNotNull("day() must not be null", cell);
        assertEquals(15L, ((Number) cell).longValue());
    }

    public void testHour() {
        Object cell = evalScalar("hour(created_at)");
        assertNotNull("hour() must not be null", cell);
        assertEquals(10L, ((Number) cell).longValue());
    }

    public void testMinute() {
        Object cell = evalScalar("minute(created_at)");
        assertNotNull("minute() must not be null", cell);
        assertEquals(30L, ((Number) cell).longValue());
    }

    public void testSecond() {
        Object cell = evalScalar("second(created_at)");
        assertNotNull("second() must not be null", cell);
        assertEquals(0L, ((Number) cell).longValue());
    }

    public void testDayOfWeek() {
        Object cell = evalScalar("dayofweek(created_at)");
        assertNotNull("dayofweek() must not be null", cell);
        assertTrue("dayofweek must be Number", cell instanceof Number);
    }

    public void testDayOfYear() {
        Object cell = evalScalar("dayofyear(created_at)");
        assertNotNull("dayofyear() must not be null", cell);
        assertTrue("dayofyear must be Number", cell instanceof Number);
    }

    public void testWeek() {
        Object cell = evalScalar("week(created_at)");
        assertNotNull("week() must not be null", cell);
        assertTrue("week must be Number", cell instanceof Number);
    }

    public void testQuarter() {
        Object cell = evalScalar("quarter(created_at)");
        assertNotNull("quarter() must not be null", cell);
        assertEquals(2L, ((Number) cell).longValue());
    }

    // ── Binning ──

    public void testSpan() {
        // SPAN appears in PPL via `stats ... by span(field, N unit)` — not callable from eval.
        // Bin created_at by 1 hour and count rows per bin.
        // Bank fixture has 2 rows on different days → expect 2 distinct bins.
        PPLRequest request = new PPLRequest(
            "source=" + BANK_INDEX + " | stats count() as cnt by span(created_at, 1h) | sort cnt"
        );
        PPLResponse response = client().execute(
            org.opensearch.ppl.action.UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("span stats response", response);
        assertEquals("two distinct hourly bins", 2, response.getRows().size());
        for (Object[] row : response.getRows()) {
            assertEquals("each bin has 1 row", 1L, ((Number) row[0]).longValue());
        }
    }

    // ── Timechart per_* functions — probing RelNode structure ──
    // These use the timechart command syntax, not eval. Separate PPL pattern.

    // testEventstatsCount — moved to WindowFunctionIT (TEAM D). Verified: passes end-to-end.

    public void testTimechartPerDay() {
        // bank fixture has created_at but timechart needs @timestamp
        // Use eval to rename, then timechart
        PPLRequest request = new PPLRequest(
            "source=" + BANK_INDEX + " | eval `@timestamp` = created_at | timechart per_day(balance)"
        );
        try {
            PPLResponse response = client().execute(
                org.opensearch.ppl.action.UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
            assertNotNull("timechart per_day response", response);
        } catch (Exception e) {
            logger.info("timechart per_day error: {}", e.getMessage());
            throw e;
        }
    }

    public void testTimechartPerHour() {
        PPLRequest request = new PPLRequest(
            "source=" + BANK_INDEX + " | eval `@timestamp` = created_at | timechart per_hour(balance)"
        );
        try {
            PPLResponse response = client().execute(
                org.opensearch.ppl.action.UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
            assertNotNull("timechart per_hour response", response);
        } catch (Exception e) {
            logger.info("timechart per_hour error: {}", e.getMessage());
            throw e;
        }
    }

    // ── Functions that may need adapters — testing PPL support ──

    public void testStrftime() {
        Object cell = evalScalar("strftime(created_at, '%Y-%m-%d')");
        assertNotNull("strftime must not be null", cell);
    }

    public void testTime() {
        Object cell = evalScalar("time(created_at)");
        assertNotNull("time() must not be null", cell);
    }

    public void testDatetime() {
        Object cell = evalScalar("datetime(created_at)");
        assertNotNull("datetime() must not be null", cell);
    }

    public void testAdddate() {
        Object cell = evalScalar("adddate(created_at, 1)");
        assertNotNull("adddate must not be null", cell);
    }

    public void testDateAdd() {
        Object cell = evalScalar("date_add(created_at, INTERVAL 1 DAY)");
        assertNotNull("date_add must not be null", cell);
    }

    public void testDateSub() {
        Object cell = evalScalar("date_sub(created_at, INTERVAL 1 DAY)");
        assertNotNull("date_sub must not be null", cell);
    }

    public void testDatediff() {
        Object cell = evalScalar("datediff(created_at, created_at)");
        assertNotNull("datediff must not be null", cell);
        assertEquals(0L, ((Number) cell).longValue());
    }

    public void testDayname() {
        Object cell = evalScalar("dayname(created_at)");
        assertNotNull("dayname must not be null", cell);
    }

    public void testMonthname() {
        Object cell = evalScalar("monthname(created_at)");
        assertNotNull("monthname must not be null", cell);
    }

    // Blocked: requires sql-repo changes that are off-limits. The PPL front-end's
    // TIMEDIFF operand checker in PPLOperandTypes accepts only (TIME, TIME) and the
    // SQL UDF is registered as "TIME_DIFF" — mismatch with our ScalarFunction.TIMEDIFF
    // enum so the DataFusion adapter never fires. Both fixes live in
    // /Users/handalm/Workspace/sql (PPLOperandTypes#TIME_TIME and PPLBuiltinOperators
    // .toUDF("TIME_DIFF") → "TIMEDIFF"). Re-enable once those land.
    @Ignore
    public void testTimediff() {
        Object cell = evalScalar("timediff(created_at, created_at)");
        assertNotNull("timediff must not be null", cell);
    }

    public void testTimestampdiff() {
        Object cell = evalScalar("timestampdiff(DAY, created_at, created_at)");
        assertNotNull("timestampdiff must not be null", cell);
        assertEquals(0L, ((Number) cell).longValue());
    }

    public void testTimestampdiffDay() {
        Object cell = evalScalar("timestampdiff(DAY, created_at, timestampadd(DAY, 3, created_at))");
        assertNotNull(cell);
        assertEquals(3L, ((Number) cell).longValue());
    }

    public void testTimestampdiffHour() {
        Object cell = evalScalar("timestampdiff(HOUR, created_at, timestampadd(HOUR, 5, created_at))");
        assertNotNull(cell);
        assertEquals(5L, ((Number) cell).longValue());
    }

    public void testTimestampdiffMillisecond() {
        // MILLISECOND must multiply epoch-seconds delta by 1000 before truncation.
        Object cell = evalScalar("timestampdiff(MILLISECOND, created_at, timestampadd(SECOND, 2, created_at))");
        assertNotNull(cell);
        assertEquals(2000L, ((Number) cell).longValue());
    }

    public void testTimestampadd() {
        // 2024-06-15T10:30:00Z + 1 day = 2024-06-16T10:30:00Z, unix_timestamp = 1718533800
        Object cell = evalScalar("unix_timestamp(timestampadd(DAY, 1, created_at))");
        assertNotNull("timestampadd must not be null", cell);
        long actual = ((Number) cell).longValue();
        logger.info("timestampadd(DAY, 1, created_at) → unix_timestamp = {}", actual);
        assertEquals("expected unix_timestamp = 2024-06-16T10:30:00Z", 1718533800L, actual);
    }

    public void testSubdate() {
        // subdate(created_at, 1) → 2024-06-14T10:30:00Z, unix_timestamp = 1718361000
        Object cell = evalScalar("unix_timestamp(subdate(created_at, 1))");
        assertNotNull("subdate must not be null", cell);
        assertEquals(1718361000L, ((Number) cell).longValue());
    }

    public void testWeekday() {
        // 2024-06-15 is a Saturday → MySQL WEEKDAY = 5
        Object cell = evalScalar("weekday(created_at)");
        assertNotNull("weekday must not be null", cell);
        assertEquals(5L, ((Number) cell).longValue());
    }

    public void testYearweek() {
        // 2024-06-15 is in week 24 of 2024 → year*100 + week = 202424
        Object cell = evalScalar("yearweek(created_at)");
        assertNotNull("yearweek must not be null", cell);
        assertEquals(202424L, ((Number) cell).longValue());
    }

    public void testMinuteOfDay() {
        // 10:30 → 10*60 + 30 = 630
        Object cell = evalScalar("minute_of_day(created_at)");
        assertNotNull("minute_of_day must not be null", cell);
        assertEquals(630L, ((Number) cell).longValue());
    }

    public void testSecToTime() {
        // 3661 seconds → 01:01:01 (maketime hours, minutes, seconds)
        Object cell = evalScalar("sec_to_time(3661)");
        assertNotNull("sec_to_time must not be null", cell);
    }

    public void testTimeToSec() {
        // maketime(1,1,1) → 3661
        Object cell = evalScalar("time_to_sec(maketime(1, 1, 1))");
        assertNotNull("time_to_sec must not be null", cell);
        assertEquals(3661L, ((Number) cell).longValue());
    }

    public void testUtcDate() {
        Object cell = evalScalar("utc_date()");
        assertNotNull("utc_date must not be null", cell);
    }

    public void testUtcTime() {
        Object cell = evalScalar("utc_time()");
        assertNotNull("utc_time must not be null", cell);
    }

    public void testUtcTimestamp() {
        Object cell = evalScalar("utc_timestamp()");
        assertNotNull("utc_timestamp must not be null", cell);
    }

    public void testLastDay() {
        // 2024-06-15 → 2024-06-30 (last day of June). unix_timestamp(2024-06-30T00:00:00Z) = 1719705600
        Object cell = evalScalar("unix_timestamp(last_day(created_at))");
        assertNotNull("last_day must not be null", cell);
        assertEquals(1719705600L, ((Number) cell).longValue());
    }

    public void testToDays() {
        // 2024-06-15T10:30:00Z → 1718447400 epoch → 1718447400/86400=19889 days since epoch
        //                     → 19889 + 719528 = 739417 days since year 0
        Object cell = evalScalar("to_days(created_at)");
        assertNotNull("to_days must not be null", cell);
        assertEquals(739417L, ((Number) cell).longValue());
    }

    public void testToSeconds() {
        // 1718447400 (epoch) + 62167219200 (year 0 → 1970-01-01) = 63885666600
        Object cell = evalScalar("to_seconds(created_at)");
        assertNotNull("to_seconds must not be null", cell);
        assertEquals(63885666600L, ((Number) cell).longValue());
    }

    public void testFromDays() {
        // from_days(737000) = 2017-11-02 → unix_timestamp = 1509580800
        Object cell = evalScalar("unix_timestamp(from_days(737000))");
        assertNotNull("from_days must not be null", cell);
        assertEquals(1509580800L, ((Number) cell).longValue());
    }

    public void testPeriodAdd() {
        // PERIOD_ADD(200801, 3) = Jan 2008 + 3 months = Apr 2008 = 200804
        Object cell = evalScalar("period_add(200801, 3)");
        assertNotNull("period_add must not be null", cell);
        assertEquals(200804L, ((Number) cell).longValue());
    }

    public void testPeriodDiff() {
        // PERIOD_DIFF(200802, 200703) = Feb 2008 - Mar 2007 = 11 months
        Object cell = evalScalar("period_diff(200802, 200703)");
        assertNotNull("period_diff must not be null", cell);
        assertEquals(11L, ((Number) cell).longValue());
    }

    public void testGetFormat() {
        // GET_FORMAT(DATE, 'USA') is pure literal substitution → "%m.%d.%Y"
        Object cell = evalScalar("get_format(DATE, 'USA')");
        assertEquals("%m.%d.%Y", cell);
    }

    public void testConvertTz() {
        // 2024-06-15T10:30:00Z shifted UTC→+10:00 → 2024-06-15T20:30:00Z, unix = 1718483400
        Object cell = evalScalar("unix_timestamp(convert_tz(created_at, '+00:00', '+10:00'))");
        assertNotNull("convert_tz must not be null", cell);
        assertEquals(1718483400L, ((Number) cell).longValue());
    }
}
