/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for arithmetic, comparison, and logical operators routed
 * through PPL → Calcite → Substrait → DataFusion. Bank fixture row 1:
 * firstname='Amber', balance=39225.
 */
public class ScalarOperatorIT extends BaseScalarFunctionIT {

    // ---- Arithmetic ----
    public void testPlus() { assertScalarLong("balance + 1", 39226L); }
    public void testMinus() { assertScalarLong("balance - 25", 39200L); }
    public void testMultiply() { assertScalarLong("balance * 2", 78450L); }
    public void testDivide() { assertScalarLong("balance / 5", 7845L); }
    public void testMod() { assertScalarLong("balance % 1000", 225L); }

    // ---- Comparison ----
    public void testEquals() { assertScalarBoolean("balance = 39225", true); }
    public void testNotEquals() { assertScalarBoolean("balance != 0", true); }
    public void testLessThan() { assertScalarBoolean("balance < 100000", true); }
    public void testLessThanOrEqual() { assertScalarBoolean("balance <= 39225", true); }
    public void testGreaterThan() { assertScalarBoolean("balance > 0", true); }
    public void testGreaterThanOrEqual() { assertScalarBoolean("balance >= 39225", true); }
    public void testLike() { assertScalarBoolean("firstname like 'A%'", true); }
    public void testBetween() { assertScalarBoolean("balance between 1 and 100000", true); }
    public void testIn() { assertScalarBoolean("balance in (39225, 1, 2)", true); }

    // ---- Logical ----
    public void testAnd() { assertScalarBoolean("balance > 0 and balance < 100000", true); }
    public void testOr() { assertScalarBoolean("balance > 100000 or balance > 0", true); }
    public void testNot() { assertScalarBoolean("not (balance > 100000)", true); }
    public void testXor() { assertScalarBoolean("(balance > 0) xor (balance > 100000)", true); }

    /** ilike: case-insensitive LIKE; PPL emits Calcite ILIKE which is YAML-aliased to substrait `like`. */
    public void testIlike() { assertScalarBoolean("ilike(firstname, 'a%')", true); }
}
