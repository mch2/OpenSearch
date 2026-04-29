/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;

/**
 * Scalar functions that a backend may support in projections and expressions.
 * Used by the project rule to verify the backend can evaluate each expression
 * in the SELECT clause.
 *
 * @opensearch.internal
 */
public enum ScalarFunction {
    // Arithmetic
    PLUS(SqlKind.PLUS),
    MINUS(SqlKind.MINUS),
    TIMES(SqlKind.TIMES),
    DIVIDE(SqlKind.DIVIDE),
    MOD(SqlKind.MOD),

    // Math
    ABS(SqlKind.OTHER),
    CEIL(SqlKind.CEIL),
    FLOOR(SqlKind.FLOOR),
    ROUND(SqlKind.OTHER),
    TRUNCATE(SqlKind.OTHER),
    SQRT(SqlKind.OTHER),
    CBRT(SqlKind.OTHER),
    EXP(SqlKind.OTHER),
    LN(SqlKind.OTHER),
    LOG(SqlKind.OTHER),
    LOG2(SqlKind.OTHER),
    LOG10(SqlKind.OTHER),
    POWER(SqlKind.OTHER),
    SIGN(SqlKind.OTHER),
    PI(SqlKind.OTHER),
    RAND(SqlKind.OTHER),
    E(SqlKind.OTHER),
    EXPM1(SqlKind.OTHER),
    RINT(SqlKind.OTHER),
    CONV(SqlKind.OTHER),

    // Trigonometric
    SIN(SqlKind.OTHER),
    COS(SqlKind.OTHER),
    COT(SqlKind.OTHER),
    SINH(SqlKind.OTHER),
    COSH(SqlKind.OTHER),
    ASIN(SqlKind.OTHER),
    ACOS(SqlKind.OTHER),
    ATAN(SqlKind.OTHER),
    ATAN2(SqlKind.OTHER),
    DEGREES(SqlKind.OTHER),
    RADIANS(SqlKind.OTHER),

    // Cast / type
    CAST(SqlKind.CAST),

    // Comparison / logical
    EQUALS(SqlKind.EQUALS),
    NOT_EQUALS(SqlKind.NOT_EQUALS),
    LESS_THAN(SqlKind.LESS_THAN),
    LESS_THAN_OR_EQUAL(SqlKind.LESS_THAN_OR_EQUAL),
    GREATER_THAN(SqlKind.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(SqlKind.GREATER_THAN_OR_EQUAL),
    LIKE(SqlKind.LIKE),
    BETWEEN(SqlKind.BETWEEN),
    SEARCH(SqlKind.SEARCH),
    AND(SqlKind.AND),
    OR(SqlKind.OR),
    NOT(SqlKind.NOT),
    IS_NULL(SqlKind.IS_NULL),
    IS_NOT_NULL(SqlKind.IS_NOT_NULL),

    // Conditional
    CASE(SqlKind.CASE),
    COALESCE(SqlKind.COALESCE),
    NULLIF(SqlKind.NULLIF),
    IF(SqlKind.OTHER);

    private final SqlKind sqlKind;

    ScalarFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Maps a Calcite SqlKind to a ScalarFunction, or null if not recognized. Skips OTHER. */
    public static ScalarFunction fromSqlKind(SqlKind kind) {
        for (ScalarFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /** Maps a function name to a ScalarFunction. Case-insensitive — Calcite operator
     *  names are typically uppercase (ABS, UPPER) while builtin DataFusion names are lowercase
     *  (abs, upper). Throws if not recognized. */
    public static ScalarFunction fromNameOrError(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized scalar function [" + name + "]", e);
        }
    }
}
