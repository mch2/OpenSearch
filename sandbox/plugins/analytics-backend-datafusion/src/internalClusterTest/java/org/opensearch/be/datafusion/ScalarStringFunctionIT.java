/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

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
}
