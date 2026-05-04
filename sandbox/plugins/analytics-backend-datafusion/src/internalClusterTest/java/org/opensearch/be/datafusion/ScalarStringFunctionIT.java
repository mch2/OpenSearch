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

/**
 * End-to-end tests for scalar string functions routed through PPL → Calcite →
 * Substrait → DataFusion. Bank fixture row 1: firstname='Amber'.
 */
public class ScalarStringFunctionIT extends BaseScalarFunctionIT {

    public void testAscii() { assertScalarLong("ascii(firstname)", 65L); }                 // 'A'
    public void testConcat() { assertScalarString("concat(firstname, '-tail')", "Amber-tail"); }
    public void testConcatWs() { assertScalarString("concat_ws('-', firstname, 'b', 'c')", "Amber-b-c"); }
    public void testLeft() { assertScalarString("left(firstname, 3)", "Amb"); }
    public void testRight() { assertScalarString("right(firstname, 3)", "ber"); }
    public void testLength() { assertScalarLong("length(firstname)", 5L); }                 // 'Amber'
    public void testLower() { assertScalarString("lower(firstname)", "amber"); }
    public void testUpper() { assertScalarString("upper(firstname)", "AMBER"); }
    public void testLtrim() { assertScalarString("ltrim(concat('   ', firstname))", "Amber"); }
    public void testRtrim() { assertScalarString("rtrim(concat(firstname, '   '))", "Amber"); }
    public void testTrim() { assertScalarString("trim(concat('  ', firstname, '  '))", "Amber"); }
    public void testSubstring() { assertScalarString("substring(firstname, 2, 3)", "mbe"); }
    public void testReverse() { assertScalarString("reverse(firstname)", "rebmA"); }
    public void testRegexpReplace() { assertScalarString("replace(firstname, 'A', 'X')", "Xmber"); }
    public void testLocate() { assertScalarLong("locate('mb', firstname)", 2L); }

    // ── strcmp / regex predicates / convert subfunctions ────────────────────
    /** PPL strcmp(a,b) → CASE three-way compare. Equal strings produce 0. */
    public void testStrcmp() { assertScalarLong("strcmp(firstname, 'Amber')", 0L); }

    /** regex_match is a PPL alias for regexp_match — both emit Calcite REGEXP_CONTAINS. */
    public void testRegexMatch() { assertScalarBoolean("regex_match(firstname, 'A.*r')", true); }

    /** regexp_match emits Calcite REGEXP_CONTAINS; aliased to DataFusion regexp_like. */
    public void testRegexpMatch() { assertScalarBoolean("regexp_match(firstname, '^A')", true); }

    // ── convert rmcomma / rmunit ──
    // These reach the backend through the `convert` command, which rewrites the named
    // field in place. RegexReplaceAdapter lowers them to regexp_replace(field, pat, '').

    /** `convert rmcomma(amount)` strips commas from a pre-existing string field. */
    public void testConvertRmcomma() {
        Object cell = runConvertFirstCell(
            "source=" + BANK_INDEX
                + " | where account_number = 1"
                + " | eval amount = '1,234,567.89'"
                + " | convert rmcomma(amount)"
                + " | fields amount"
                + " | head 1");
        assertNotNull("convert rmcomma result must not be null", cell);
        assertEquals("convert rmcomma(amount)", "1234567.89", cell.toString());
    }

    /**
     * `convert rmunit(distance)` strips trailing alphabetic units and coerces to a numeric
     * string. PPL's convert subfunction casts the stripped value to a double, so "100km"
     * becomes "100.0" (not "100").
     */
    public void testConvertRmunit() {
        Object cell = runConvertFirstCell(
            "source=" + BANK_INDEX
                + " | where account_number = 1"
                + " | eval distance = '100km'"
                + " | convert rmunit(distance)"
                + " | fields distance"
                + " | head 1");
        assertNotNull("convert rmunit result must not be null", cell);
        assertEquals("convert rmunit(distance)", "100.0", cell.toString());
    }

    // Unreachable in PPL — intentionally not tested:
    //   `regexp(a, b)` (prefix form): PPL only accepts `a REGEXP b` infix. testRegexp()
    //     covers the infix form, and regexp_match(...) covers the same predicate surface.
    //   `a || b` string concat: not in the PPL lexer. testConcat() covers concat(a, b).

    /** Minimal raw-PPL runner for convert-command tests (returns the first row's only cell). */
    private Object runConvertFirstCell(String ppl) {
        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        Object[] row = response.getRows().get(0);
        assertTrue("row must have at least 1 column", row.length >= 1);
        return row[0];
    }
}
