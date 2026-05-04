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

    // ── typeof ────────────────────────────────────────────────────────
    // PPL folds typeof(x) to a string literal at parse time using Calcite's static
    // type info — no backend wiring needed. Verifying the literal survives the
    // PPL → Calcite → substrait → DataFusion → result roundtrip.
    public void testTypeofLong() { assertScalarString("typeof(balance)", "BIGINT"); }

    // ── num / number_to_string ────────────────────────────────────────
    // PPL's `num` is accepted only inside the `convert` command (e.g.
    // `source=bank | convert num(balance) as b`). Backend wiring (NUM → ToNumberAdapter)
    // means the convert command produces a plan DataFusion can execute.

    /** `convert num(balance)` rewrites `balance` in place → CAST AS DOUBLE via ToNumberAdapter. */
    public void testConvertNum() {
        Object cell = runConvertFirstCell(
            "source=" + BANK_INDEX
                + " | where account_number = 1"
                + " | convert num(balance)"
                + " | fields balance"
                + " | head 1");
        assertNotNull("convert num(balance) result must not be null", cell);
        assertTrue("must be Number, got " + cell.getClass(), cell instanceof Number);
        assertEquals("convert num(balance)", 39225.0, ((Number) cell).doubleValue(), 1e-9);
    }

    // number_to_string is only emitted implicitly by Calcite's type coercion when
    // a number flows into a string-typed slot. It has no surface PPL name; testing
    // it directly requires either a coerced binary CONCAT with a numeric arg (which
    // PPL rejects at type-check) or a low-level plan probe. The explicit cast tested
    // by testCastDouble + testToStringFromInteger already exercises the same
    // ToStringAdapter rewrite path.

    /** Minimal raw-PPL runner for tests that don't fit the eval template. */
    private Object runConvertFirstCell(String ppl) {
        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("PPLResponse must not be null", response);
        assertEquals("head 1 → exactly 1 row", 1, response.getRows().size());
        Object[] row = response.getRows().get(0);
        assertTrue("row must have at least 1 column", row.length >= 1);
        List<String> cols = response.getColumns();
        assertEquals("expected single-column projection", 1, cols.size());
        return row[0];
    }

    // ── cast (explicit) ──────────────────────────────────────────────
    /** Explicit cast(x AS DOUBLE) routes through Calcite CAST — already handled by SafeDivisionTransformer-free path. */
    public void testCastDouble() { assertScalarDouble("cast(balance as double)", 39225.0, 1e-9); }
}
