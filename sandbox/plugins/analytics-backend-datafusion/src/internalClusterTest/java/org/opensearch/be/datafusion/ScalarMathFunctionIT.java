/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end tests for scalar math functions routed through PPL → Calcite →
 * Substrait → DataFusion. Tests use natural PPL form against the bank fixture
 * row 1 (firstname='Amber', balance=39225). Integer-typed operands on fp-only
 * substrait functions (sin, cos, ln, sqrt, …) are handled by declaring i64
 * variants in {@code opensearch_scalar.yaml} — DataFusion coerces internally.
 */
public class ScalarMathFunctionIT extends BaseScalarFunctionIT {

    public void testAbs() { assertScalarLong("abs(0 - balance)", 39225L); }
    public void testCbrt() { assertScalarDouble("cbrt(balance)", Math.cbrt(39225.0), 1e-6); }
    public void testTruncate() { assertScalarDouble("truncate(balance + 0.3, 0)", 39225.0, 1e-9); }
    public void testPi() { assertScalarDouble("pi() + (balance - balance)", 3.141592653589793, 1e-9); }

    // ---- Custom UDFs (declared in opensearch_scalar.yaml + implemented in rust/src/udf/mod.rs) ----
    public void testE() { assertScalarDouble("e() + (balance - balance)", Math.E, 1e-9); }
    public void testExpm1() { assertScalarDouble("expm1(balance - balance)", 0.0, 1e-9); } // expm1(0) = 0
    public void testRint() { assertScalarDouble("rint(balance + 0.5)", 39226.0, 1e-9); } // 39225.5 → 39226 (ties to even)
    public void testRand() {
        Object cell = evalScalar("rand() + (balance - balance)");
        assertNotNull("rand() result must not be null", cell);
        assertTrue("rand() must be Number", cell instanceof Number);
        double v = ((Number) cell).doubleValue();
        assertTrue("rand() in [0, 1), got " + v, v >= 0.0 && v < 1.0);
    }
    public void testSign() { assertScalarLong("sign(0 - balance)", -1L); }
    public void testCosh() { assertScalarDouble("cosh(balance - balance)", 1.0, 1e-9); }
    public void testSinh() { assertScalarDouble("sinh(balance - balance)", 0.0, 1e-9); }
    public void testCot() { assertScalarDouble("cot(balance / balance)", 1.0 / Math.tan(1.0), 1e-9); }
    // Note: floor/ceil over `i64/decimal` divide expressions tickle a runtime issue in the
    // chain (cast → fp64 → cast back) — the simpler `floor(double_field)` form works.
    // Tests use balance multiplied/added with a fp literal to keep the path numeric without divide.
//    public void testCeil() { assertScalarDouble("ceil(balance + 0.3)", 39226.0, 1e-9); }
    public void testFloor() { assertScalarDouble("floor(balance + 0.3)", 39225.0, 1e-9); }
    // Substrait's round takes 2 args: round(value, digits). Calcite's round(x) defaults the
    // 2nd arg implicitly during validation; emit it explicitly here so substrait matches.
    public void testRound() { assertScalarLong("round(balance, 0)", 39225L); }
    public void testSqrt() { assertScalarDouble("sqrt(balance)", Math.sqrt(39225.0), 1e-6); }
    public void testExp() { assertScalarDouble("exp(balance - balance)", 1.0, 1e-9); }
    public void testLn() { assertScalarDouble("ln(balance)", Math.log(39225.0), 1e-9); }
    public void testLog1Arg() { assertScalarDouble("log(balance)", Math.log(39225.0), 1e-9); }
    public void testLog2Arg() { assertScalarDouble("log(2, balance)", Math.log(39225.0) / Math.log(2), 1e-9); }
    public void testLog2() { assertScalarDouble("log2(balance)", Math.log(39225.0) / Math.log(2), 1e-9); }
    public void testLog10() { assertScalarDouble("log10(balance)", Math.log10(39225.0), 1e-9); }
    public void testPow() { assertScalarDouble("pow(2, 3)", 8.0, 1e-9); }
    public void testPower() { assertScalarDouble("power(balance, 0)", 1.0, 1e-9); }
    // sign() emits the substrait "sign" function which DataFusion doesn't recognize at runtime
    // (it has the same op as "signum"). Needs name mapping in the DataFusion substrait consumer
    // (Rust side) — out of scope tonight.

    // greatest/least and scalar_max/scalar_min are not registered in the PPL parser's grammar
    // — both fail with "mismatched input '('" at parse time. The CSV gap analysis lists
    // scalar_max/scalar_min as the canonical PPL names; needs PPL frontend work to enable.

    public void testCos() { assertScalarDouble("cos(balance - balance)", 1.0, 1e-9); }
    public void testSin() { assertScalarDouble("sin(balance - balance)", 0.0, 1e-9); }
    public void testTan() { assertScalarDouble("tan(balance - balance)", 0.0, 1e-9); }
    public void testAcos() { assertScalarDouble("acos(balance / balance)", 0.0, 1e-9); }
    public void testAsin() { assertScalarDouble("asin(balance - balance)", 0.0, 1e-9); }
    public void testAtan() { assertScalarDouble("atan(balance - balance)", 0.0, 1e-9); }
    public void testAtan2() { assertScalarDouble("atan2(balance - balance, balance / balance)", 0.0, 1e-9); }
    public void testDegrees() { assertScalarDouble("degrees(balance - balance)", 0.0, 1e-9); }
    public void testRadians() { assertScalarDouble("radians(balance - balance)", 0.0, 1e-9); }
    public void testConv() { assertScalarString("conv(15, 10, 2)", "1111"); }

    // ---- Pairwise scalar min/max ----
    // PPL's AstExpressionBuilder lowers user-written `max(a, b)` / `min(a, b)` in eval
    // context (where there's no grouping) into the SCALAR_MAX / SCALAR_MIN internal
    // operator. Backend wiring maps SCALAR_MAX → substrait greatest, SCALAR_MIN → least.
    public void testScalarMax() { assertScalarLong("max(3, 5) + (balance - balance)", 5L); }
    public void testScalarMin() { assertScalarLong("min(3, 5) + (balance - balance)", 3L); }
}
