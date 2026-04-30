/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for type-conversion functions {@code tonumber} and {@code tostring}.
 *
 * <p>PPL emits these as UDF calls; the {@link ToNumberAdapter} / {@link ToStringAdapter}
 * registered with {@code BackendPlanAdapter} rewrite them to {@code CAST AS DOUBLE} /
 * {@code CAST AS VARCHAR} before substrait conversion. The tests verify the rewrite
 * produces a plan DataFusion can execute and the resulting value matches.
 *
 * <p>Bank fixture row 1: account_number=1, firstname='Amber', balance=39225.
 */
public class ScalarConversionFunctionIT extends BaseScalarFunctionIT {

    // ── tonumber(string) → DOUBLE ─────────────────────────────────────
    // PPL's tonumber type-checks to string input; numeric inputs are rejected
    // by the frontend before reaching us. Real-world use is parsing a stringy field.

    public void testToNumberFromIntegerLiteral() {
        assertScalarDouble("tonumber('39225')", 39225.0, 1e-9);
    }

    public void testToNumberFromDecimalLiteral() {
        assertScalarDouble("tonumber('123.5')", 123.5, 1e-9);
    }

    public void testToNumberFromNegative() {
        assertScalarDouble("tonumber('-42.0')", -42.0, 1e-9);
    }

    public void testToNumberFromStringField() {
        // tostring → tonumber roundtrip, verifying both adapters compose correctly.
        assertScalarDouble("tonumber(tostring(balance))", 39225.0, 1e-9);
    }

    // ── tostring(*) → VARCHAR ─────────────────────────────────────────

    /** Integer column → string. */
    public void testToStringFromInteger() {
        assertScalarString("tostring(balance)", "39225");
    }

    /** String pass-through (CAST string→varchar is identity). */
    public void testToStringFromString() {
        assertScalarString("tostring(firstname)", "Amber");
    }

    /** Numeric expression → string. */
    public void testToStringFromExpression() {
        assertScalarString("tostring(balance + 0)", "39225");
    }
}
