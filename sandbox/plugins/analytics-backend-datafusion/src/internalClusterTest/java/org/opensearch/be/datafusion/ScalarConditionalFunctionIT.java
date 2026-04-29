/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for conditional / null-handling functions routed through
 * PPL → Calcite → Substrait → DataFusion. Bank fixture row 1:
 * firstname='Amber', balance=39225.
 */
public class ScalarConditionalFunctionIT extends BaseScalarFunctionIT {

    public void testIfTrue() { assertScalarString("if(balance > 0, firstname, 'never')", "Amber"); }
    public void testIfFalse() { assertScalarString("if(balance < 0, 'never', firstname)", "Amber"); }
    public void testIfnullPicksValue() { assertScalarString("ifnull(firstname, 'fallback')", "Amber"); }
    public void testIfnullPicksDefault() { assertScalarString("ifnull(nullif(firstname, firstname), 'fallback')", "fallback"); }
    public void testCoalescePicksFirstNonNull() {
        assertScalarString("coalesce(nullif(firstname, firstname), firstname, 'last')", "Amber");
    }
    public void testNullifReturnsNullWhenEqual() { assertScalarNull("nullif(firstname, firstname)"); }
    public void testNullifReturnsValueWhenDifferent() { assertScalarString("nullif(firstname, 'other')", "Amber"); }
    public void testIsnullOnNull() { assertScalarBoolean("isnull(nullif(firstname, firstname))", true); }
    public void testIsnotnullOnValue() { assertScalarBoolean("isnotnull(firstname)", true); }
    public void testIspresentOnValue() { assertScalarBoolean("ispresent(firstname)", true); }
}
