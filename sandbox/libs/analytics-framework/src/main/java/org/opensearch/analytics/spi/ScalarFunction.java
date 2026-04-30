/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.Locale;

/**
 * All scalar functions a backend may support — comparisons, full-text, math, string,
 * conditional, date/time, and cast operations. Used across filter, project, and
 * aggregate expression capability declarations.
 *
 * <p>Each function carries a {@link Category} indicating its type and whether
 * it supports parameters (e.g., full-text operators accept analyzer, slop, etc.).
 *
 * @opensearch.internal
 */
public enum ScalarFunction {

    // ── Comparisons ──────────────────────────────────────────────────
    EQUALS(Category.COMPARISON, SqlKind.EQUALS),
    NOT_EQUALS(Category.COMPARISON, SqlKind.NOT_EQUALS),
    GREATER_THAN(Category.COMPARISON, SqlKind.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(Category.COMPARISON, SqlKind.GREATER_THAN_OR_EQUAL),
    LESS_THAN(Category.COMPARISON, SqlKind.LESS_THAN),
    LESS_THAN_OR_EQUAL(Category.COMPARISON, SqlKind.LESS_THAN_OR_EQUAL),
    IS_NULL(Category.COMPARISON, SqlKind.IS_NULL),
    IS_NOT_NULL(Category.COMPARISON, SqlKind.IS_NOT_NULL),
    IS_TRUE(Category.COMPARISON, SqlKind.IS_TRUE),
    IS_FALSE(Category.COMPARISON, SqlKind.IS_FALSE),
    IS_NOT_TRUE(Category.COMPARISON, SqlKind.IS_NOT_TRUE),
    IS_NOT_FALSE(Category.COMPARISON, SqlKind.IS_NOT_FALSE),
    IN(Category.COMPARISON, SqlKind.IN),
    LIKE(Category.COMPARISON, SqlKind.LIKE),
    BETWEEN(Category.COMPARISON, SqlKind.BETWEEN),
    SEARCH(Category.COMPARISON, SqlKind.SEARCH),
    AND(Category.COMPARISON, SqlKind.AND),
    OR(Category.COMPARISON, SqlKind.OR),
    NOT(Category.COMPARISON, SqlKind.NOT),
    PREFIX(Category.COMPARISON, SqlKind.OTHER),

    // ── Full-text search ─────────────────────────────────────────────
    MATCH(Category.FULL_TEXT, SqlKind.OTHER),
    MATCH_PHRASE(Category.FULL_TEXT, SqlKind.OTHER),
    FUZZY(Category.FULL_TEXT, SqlKind.OTHER),
    WILDCARD(Category.FULL_TEXT, SqlKind.OTHER),
    REGEXP(Category.FULL_TEXT, SqlKind.OTHER),

    // ── String ───────────────────────────────────────────────────────
    UPPER(Category.STRING, SqlKind.OTHER),
    LOWER(Category.STRING, SqlKind.OTHER),
    TRIM(Category.STRING, SqlKind.TRIM),
    LTRIM(Category.STRING, SqlKind.OTHER),
    RTRIM(Category.STRING, SqlKind.OTHER),
    SUBSTRING(Category.STRING, SqlKind.OTHER),
    CONCAT(Category.STRING, SqlKind.OTHER),
    CONCAT_WS(Category.STRING, SqlKind.OTHER),
    CHAR_LENGTH(Category.STRING, SqlKind.OTHER),
    LEFT(Category.STRING, SqlKind.OTHER),
    RIGHT(Category.STRING, SqlKind.OTHER),
    REVERSE(Category.STRING, SqlKind.OTHER),
    REPLACE(Category.STRING, SqlKind.OTHER),
    REGEXP_REPLACE(Category.STRING, SqlKind.OTHER),
    ASCII(Category.STRING, SqlKind.OTHER),
    LOCATE(Category.STRING, SqlKind.OTHER),
    POSITION(Category.STRING, SqlKind.OTHER),
    LEN(Category.STRING, SqlKind.OTHER),
    LENGTH(Category.STRING, SqlKind.OTHER),
    MD5(Category.STRING, SqlKind.OTHER),
    SHA1(Category.STRING, SqlKind.OTHER),
    SHA2(Category.STRING, SqlKind.OTHER),
    SHA256(Category.STRING, SqlKind.OTHER),
    JSON_VALID(Category.STRING, SqlKind.OTHER),
    JSON_OBJECT(Category.STRING, SqlKind.OTHER),
    CIDRMATCH(Category.COMPARISON, SqlKind.OTHER),
    // Multi-value functions (mapped to DF array_* via calcite_aliases)
    MVCOUNT(Category.STRING, SqlKind.OTHER),
    MVJOIN(Category.STRING, SqlKind.OTHER),
    MVINDEX(Category.STRING, SqlKind.OTHER),
    MVAPPEND(Category.STRING, SqlKind.OTHER),
    MVDEDUP(Category.STRING, SqlKind.OTHER),
    SPLIT(Category.STRING, SqlKind.OTHER),

    // ── Math ─────────────────────────────────────────────────────────
    PLUS(Category.MATH, SqlKind.PLUS),
    MINUS(Category.MATH, SqlKind.MINUS),
    TIMES(Category.MATH, SqlKind.TIMES),
    DIVIDE(Category.MATH, SqlKind.DIVIDE),
    MOD(Category.MATH, SqlKind.MOD),
    ABS(Category.MATH, SqlKind.OTHER),
    CEIL(Category.MATH, SqlKind.CEIL),
    FLOOR(Category.MATH, SqlKind.FLOOR),
    ROUND(Category.MATH, SqlKind.OTHER),
    TRUNCATE(Category.MATH, SqlKind.OTHER),
    SQRT(Category.MATH, SqlKind.OTHER),
    CBRT(Category.MATH, SqlKind.OTHER),
    EXP(Category.MATH, SqlKind.OTHER),
    LN(Category.MATH, SqlKind.OTHER),
    LOG(Category.MATH, SqlKind.OTHER),
    LOG2(Category.MATH, SqlKind.OTHER),
    LOG10(Category.MATH, SqlKind.OTHER),
    POWER(Category.MATH, SqlKind.OTHER),
    SIGN(Category.MATH, SqlKind.OTHER),
    PI(Category.MATH, SqlKind.OTHER),
    RAND(Category.MATH, SqlKind.OTHER),
    E(Category.MATH, SqlKind.OTHER),
    EXPM1(Category.MATH, SqlKind.OTHER),
    RINT(Category.MATH, SqlKind.OTHER),
    CONV(Category.MATH, SqlKind.OTHER),
    CRC32(Category.MATH, SqlKind.OTHER),
    LOGB(Category.MATH, SqlKind.OTHER),
    SIGNUM(Category.MATH, SqlKind.OTHER),
    TRUNC(Category.MATH, SqlKind.OTHER),
    RANDOM(Category.MATH, SqlKind.OTHER),

    // ── Trigonometric ────────────────────────────────────────────────
    SIN(Category.MATH, SqlKind.OTHER),
    COS(Category.MATH, SqlKind.OTHER),
    TAN(Category.MATH, SqlKind.OTHER),
    COT(Category.MATH, SqlKind.OTHER),
    SINH(Category.MATH, SqlKind.OTHER),
    COSH(Category.MATH, SqlKind.OTHER),
    TANH(Category.MATH, SqlKind.OTHER),
    ASIN(Category.MATH, SqlKind.OTHER),
    ACOS(Category.MATH, SqlKind.OTHER),
    ATAN(Category.MATH, SqlKind.OTHER),
    ATAN2(Category.MATH, SqlKind.OTHER),
    DEGREES(Category.MATH, SqlKind.OTHER),
    RADIANS(Category.MATH, SqlKind.OTHER),

    // ── Cast / type ──────────────────────────────────────────────────
    CAST(Category.TYPE, SqlKind.CAST),

    // ── Conditional ──────────────────────────────────────────────────
    CASE(Category.CONDITIONAL, SqlKind.CASE),
    COALESCE(Category.CONDITIONAL, SqlKind.COALESCE),
    NULLIF(Category.CONDITIONAL, SqlKind.NULLIF),
    IF(Category.CONDITIONAL, SqlKind.OTHER),

    // ── Date/time — zero-arg or direct DF built-in match ─────────────
    DATE(Category.DATETIME, SqlKind.OTHER),
    NOW(Category.DATETIME, SqlKind.OTHER),
    CURRENT_DATE(Category.DATETIME, SqlKind.OTHER),
    CURRENT_TIME(Category.DATETIME, SqlKind.OTHER),
    CURRENT_TIMESTAMP(Category.DATETIME, SqlKind.OTHER),
    SYSDATE(Category.DATETIME, SqlKind.OTHER),
    FROM_UNIXTIME(Category.DATETIME, SqlKind.OTHER),
    UNIX_TIMESTAMP(Category.DATETIME, SqlKind.OTHER),
    TO_UNIXTIME(Category.DATETIME, SqlKind.OTHER),
    MAKE_DATE(Category.DATETIME, SqlKind.OTHER),
    MAKE_TIME(Category.DATETIME, SqlKind.OTHER),
    TO_DATE(Category.DATETIME, SqlKind.OTHER),
    TO_TIME(Category.DATETIME, SqlKind.OTHER),
    TO_TIMESTAMP(Category.DATETIME, SqlKind.OTHER),
    TIMESTAMP(Category.DATETIME, SqlKind.OTHER),
    TO_CHAR(Category.DATETIME, SqlKind.OTHER),
    DATE_FORMAT(Category.DATETIME, SqlKind.OTHER),
    EXTRACT(Category.DATETIME, SqlKind.EXTRACT),
    DATE_PART(Category.DATETIME, SqlKind.OTHER),
    STRFTIME(Category.DATETIME, SqlKind.OTHER),
    STRPTIME(Category.DATETIME, SqlKind.OTHER),
    TIME(Category.DATETIME, SqlKind.OTHER),
    DATETIME(Category.DATETIME, SqlKind.OTHER),

    // ── Conversion (mapped to CAST in PPL frontend) ──────────────────
    TONUMBER(Category.TYPE, SqlKind.OTHER),
    TOSTRING(Category.TYPE, SqlKind.OTHER);

    /**
     * Category of scalar function.
     */
    public enum Category {
        COMPARISON(false),
        FULL_TEXT(true),
        STRING(false),
        MATH(false),
        TYPE(false),
        CONDITIONAL(false),
        DATETIME(false);

        private final boolean supportsParams;

        Category(boolean supportsParams) {
            this.supportsParams = supportsParams;
        }

        public boolean supportsParams() {
            return supportsParams;
        }
    }

    private final Category category;
    private final SqlKind sqlKind;

    ScalarFunction(Category category, SqlKind sqlKind) {
        this.category = category;
        this.sqlKind = sqlKind;
    }

    public Category getCategory() {
        return category;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /**
     * Maps a Calcite SqlKind to a ScalarFunction, or null if not recognized.
     * Skips OTHER to avoid ambiguity (multiple functions share OTHER).
     */
    public static ScalarFunction fromSqlKind(SqlKind kind) {
        for (ScalarFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /** Maps a Calcite SqlFunction to a ScalarFunction by name, or null if not recognized. */
    public static ScalarFunction fromSqlFunction(SqlFunction function) {
        try {
            return ScalarFunction.valueOf(function.getName().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
