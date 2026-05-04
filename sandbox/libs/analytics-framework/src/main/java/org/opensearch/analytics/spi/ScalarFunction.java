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
 * All scalar functions a backend may support — comparisons, full-text search,
 * math, string, conditional, date/time, and cast operations. Used across filter,
 * project, and aggregate expression capability declarations.
 *
 * <p>Each function carries a {@link Category} indicating its type. SCALAR is a
 * catch-all for functions that don't fit comparison/full-text/string/math
 * (CAST, CASE, COALESCE, EXTRACT, datetime, conversion, etc.).
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
    ILIKE(Category.COMPARISON, SqlKind.OTHER),
    BETWEEN(Category.COMPARISON, SqlKind.BETWEEN),
    SEARCH(Category.COMPARISON, SqlKind.SEARCH),
    AND(Category.COMPARISON, SqlKind.AND),
    OR(Category.COMPARISON, SqlKind.OR),
    NOT(Category.COMPARISON, SqlKind.NOT),
    PREFIX(Category.COMPARISON, SqlKind.OTHER),
    CIDRMATCH(Category.COMPARISON, SqlKind.OTHER),
    EQUALS_IP(Category.COMPARISON, SqlKind.OTHER),
    NOT_EQUALS_IP(Category.COMPARISON, SqlKind.OTHER),
    LESS_IP(Category.COMPARISON, SqlKind.OTHER),
    LTE_IP(Category.COMPARISON, SqlKind.OTHER),
    GREATER_IP(Category.COMPARISON, SqlKind.OTHER),
    GTE_IP(Category.COMPARISON, SqlKind.OTHER),

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
    JSON(Category.STRING, SqlKind.OTHER),
    JSON_ARRAY(Category.STRING, SqlKind.OTHER),
    JSON_ARRAY_LENGTH(Category.STRING, SqlKind.OTHER),
    JSON_KEYS(Category.STRING, SqlKind.OTHER),
    JSON_EXTRACT(Category.STRING, SqlKind.OTHER),
    JSON_EXTRACT_ALL(Category.STRING, SqlKind.OTHER),
    JSON_SET(Category.STRING, SqlKind.OTHER),
    JSON_DELETE(Category.STRING, SqlKind.OTHER),
    JSON_APPEND(Category.STRING, SqlKind.OTHER),
    JSON_EXTEND(Category.STRING, SqlKind.OTHER),
    // Multi-value functions (mapped to DF array_* via calcite_aliases)
    MVCOUNT(Category.STRING, SqlKind.OTHER),
    MVJOIN(Category.STRING, SqlKind.OTHER),
    MVINDEX(Category.STRING, SqlKind.OTHER),
    MVAPPEND(Category.STRING, SqlKind.OTHER),
    MVDEDUP(Category.STRING, SqlKind.OTHER),
    MVZIP(Category.STRING, SqlKind.OTHER),
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

    // ── Cast / type / conditional / datetime / conversion (SCALAR catch-all) ──
    CAST(Category.SCALAR, SqlKind.CAST),
    CASE(Category.SCALAR, SqlKind.CASE),
    COALESCE(Category.SCALAR, SqlKind.COALESCE),
    NULLIF(Category.SCALAR, SqlKind.NULLIF),
    IF(Category.SCALAR, SqlKind.OTHER),

    // Date/time
    DATE(Category.SCALAR, SqlKind.OTHER),
    NOW(Category.SCALAR, SqlKind.OTHER),
    CURRENT_DATE(Category.SCALAR, SqlKind.OTHER),
    CURRENT_TIME(Category.SCALAR, SqlKind.OTHER),
    CURRENT_TIMESTAMP(Category.SCALAR, SqlKind.OTHER),
    SYSDATE(Category.SCALAR, SqlKind.OTHER),
    FROM_UNIXTIME(Category.SCALAR, SqlKind.OTHER),
    UNIX_TIMESTAMP(Category.SCALAR, SqlKind.OTHER),
    TO_UNIXTIME(Category.SCALAR, SqlKind.OTHER),
    MAKE_DATE(Category.SCALAR, SqlKind.OTHER),
    MAKE_TIME(Category.SCALAR, SqlKind.OTHER),
    TO_DATE(Category.SCALAR, SqlKind.OTHER),
    TO_TIME(Category.SCALAR, SqlKind.OTHER),
    TO_TIMESTAMP(Category.SCALAR, SqlKind.OTHER),
    TIMESTAMP(Category.SCALAR, SqlKind.OTHER),
    TO_CHAR(Category.SCALAR, SqlKind.OTHER),
    DATE_FORMAT(Category.SCALAR, SqlKind.OTHER),
    EXTRACT(Category.SCALAR, SqlKind.EXTRACT),
    DATE_PART(Category.SCALAR, SqlKind.OTHER),
    STRFTIME(Category.SCALAR, SqlKind.OTHER),
    STRPTIME(Category.SCALAR, SqlKind.OTHER),
    TIME(Category.SCALAR, SqlKind.OTHER),
    DATETIME(Category.SCALAR, SqlKind.OTHER),

    // Date-part extraction (rewritten to EXTRACT by DatePartAdapter)
    YEAR(Category.SCALAR, SqlKind.OTHER),
    MONTH(Category.SCALAR, SqlKind.OTHER),
    MONTH_OF_YEAR(Category.SCALAR, SqlKind.OTHER),
    DAY(Category.SCALAR, SqlKind.OTHER),
    DAYOFMONTH(Category.SCALAR, SqlKind.OTHER),
    DAY_OF_MONTH(Category.SCALAR, SqlKind.OTHER),
    HOUR(Category.SCALAR, SqlKind.OTHER),
    HOUR_OF_DAY(Category.SCALAR, SqlKind.OTHER),
    MINUTE(Category.SCALAR, SqlKind.OTHER),
    MINUTE_OF_HOUR(Category.SCALAR, SqlKind.OTHER),
    SECOND(Category.SCALAR, SqlKind.OTHER),
    SECOND_OF_MINUTE(Category.SCALAR, SqlKind.OTHER),
    DAYOFWEEK(Category.SCALAR, SqlKind.OTHER),
    DAY_OF_WEEK(Category.SCALAR, SqlKind.OTHER),
    DAYOFYEAR(Category.SCALAR, SqlKind.OTHER),
    DAY_OF_YEAR(Category.SCALAR, SqlKind.OTHER),
    WEEK(Category.SCALAR, SqlKind.OTHER),
    WEEKOFYEAR(Category.SCALAR, SqlKind.OTHER),
    WEEK_OF_YEAR(Category.SCALAR, SqlKind.OTHER),
    QUARTER(Category.SCALAR, SqlKind.OTHER),
    MICROSECOND(Category.SCALAR, SqlKind.OTHER),

    // Date arithmetic + formatting (rewritten by adapters)
    ADDDATE(Category.SCALAR, SqlKind.OTHER),
    SUBDATE(Category.SCALAR, SqlKind.OTHER),
    DATE_ADD(Category.SCALAR, SqlKind.OTHER),
    DATE_SUB(Category.SCALAR, SqlKind.OTHER),
    ADDTIME(Category.SCALAR, SqlKind.OTHER),
    SUBTIME(Category.SCALAR, SqlKind.OTHER),
    DATEDIFF(Category.SCALAR, SqlKind.OTHER),
    TIMEDIFF(Category.SCALAR, SqlKind.OTHER),
    TIMESTAMPADD(Category.SCALAR, SqlKind.OTHER),
    TIMESTAMPDIFF(Category.SCALAR, SqlKind.OTHER),
    DAYNAME(Category.SCALAR, SqlKind.OTHER),
    MONTHNAME(Category.SCALAR, SqlKind.OTHER),
    LAST_DAY(Category.SCALAR, SqlKind.OTHER),
    STR_TO_DATE(Category.SCALAR, SqlKind.OTHER),
    TIME_TO_SEC(Category.SCALAR, SqlKind.OTHER),
    SEC_TO_TIME(Category.SCALAR, SqlKind.OTHER),
    CONVERT_TZ(Category.SCALAR, SqlKind.OTHER),
    WEEKDAY(Category.SCALAR, SqlKind.OTHER),
    YEARWEEK(Category.SCALAR, SqlKind.OTHER),
    MINUTE_OF_DAY(Category.SCALAR, SqlKind.OTHER),
    UTC_DATE(Category.SCALAR, SqlKind.OTHER),
    UTC_TIME(Category.SCALAR, SqlKind.OTHER),
    UTC_TIMESTAMP(Category.SCALAR, SqlKind.OTHER),
    FROM_DAYS(Category.SCALAR, SqlKind.OTHER),
    TO_DAYS(Category.SCALAR, SqlKind.OTHER),
    TO_SECONDS(Category.SCALAR, SqlKind.OTHER),
    PERIOD_ADD(Category.SCALAR, SqlKind.OTHER),
    PERIOD_DIFF(Category.SCALAR, SqlKind.OTHER),
    GET_FORMAT(Category.SCALAR, SqlKind.OTHER),

    // Binning. SPAN: PPL visitor emits OPENSEARCH_SPAN Rust UDF directly.
    // This enum entry is kept as a capability-declaration anchor but is not
    // on the live emission path.
    SPAN(Category.SCALAR, SqlKind.OTHER),
    SPAN_BUCKET(Category.SCALAR, SqlKind.OTHER),
    WIDTH_BUCKET(Category.SCALAR, SqlKind.OTHER),
    RANGE_BUCKET(Category.SCALAR, SqlKind.OTHER),
    MINSPAN_BUCKET(Category.SCALAR, SqlKind.OTHER),

    // PPL's string-to-IP cast. Calcite emits this as a named UDF wrapping the
    // literal on one side of an IP comparison (e.g. `where ip_field = '1.2.3.4'`
    // becomes `equals_ip(ip_field, IP('1.2.3.4'))`). Rewritten to a no-op by
    // IpCastAdapter since the downstream UDFs accept Utf8 strings directly.
    IP(Category.SCALAR, SqlKind.OTHER),

    // Conversion (rewritten to CAST by ToNumberAdapter / ToStringAdapter)
    TONUMBER(Category.SCALAR, SqlKind.OTHER),
    TOSTRING(Category.SCALAR, SqlKind.OTHER),
    /** PPL alias for {@code tonumber}. Rewritten to {@code CAST AS DOUBLE}. */
    NUM(Category.SCALAR, SqlKind.OTHER),
    /** PPL alias for {@code tostring}. Rewritten to {@code CAST AS VARCHAR}. */
    NUMBER_TO_STRING(Category.SCALAR, SqlKind.OTHER),

    // String adapters (PPL UDFs not in DataFusion / substrait core)
    /** PPL {@code strcmp(a,b)} → {@code CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END}. */
    STRCMP(Category.STRING, SqlKind.OTHER),
    /** PPL convert subfunction {@code rmcomma(s)} → {@code regexp_replace(s, ',', '')}. */
    RMCOMMA(Category.STRING, SqlKind.OTHER),
    /** PPL convert subfunction {@code rmunit(s)} → {@code regexp_replace(s, '[A-Za-z]+$', '')}. */
    RMUNIT(Category.STRING, SqlKind.OTHER),

    // Pairwise min/max (PPL UDFs SCALAR_MAX / SCALAR_MIN)
    SCALAR_MAX(Category.MATH, SqlKind.OTHER),
    SCALAR_MIN(Category.MATH, SqlKind.OTHER),

    /** Calcite {@code REGEXP_CONTAINS(value, pattern)} (PPL {@code regexp_match} / {@code regex_match}). */
    REGEXP_CONTAINS(Category.STRING, SqlKind.OTHER),

    /**
     * Calcite's {@code ITEM($container, $index)} — used both for struct field access
     * and array element access (PPL's {@code mvindex} lowers to this for arrays).
     * Backends may adapt the array-typed variant to a backend-native {@code array_element}
     * while passing struct-typed variants through untouched.
     */
    ITEM(Category.SCALAR, SqlKind.ITEM),

    /** PPL UDF {@code REX_EXTRACT(field, pattern, group_name)} emitted by the {@code rex} command
     *  for each named capture group. Backends may rewrite to e.g.
     *  {@code array_element(regexp_match(...), index)} when they can resolve the named group's
     *  1-based position at plan time (pattern must be a literal). */
    REX_EXTRACT(Category.STRING, SqlKind.OTHER),

    /**
     * Calcite's {@code ARRAY_COMPACT(array)} — removes NULL elements from {@code array}.
     * Emitted by PPL's {@code nomv} command via {@code array_compact(field)}. Backends
     * without a native {@code array_compact} may rewrite it (e.g. DataFusion's
     * {@code array_remove_all(array, CAST(NULL AS T))}).
     */
    ARRAY_COMPACT(Category.SCALAR, SqlKind.ARRAY_COMPACT);

    /**
     * Category of scalar function.
     */
    public enum Category {
        COMPARISON,
        FULL_TEXT,
        STRING,
        MATH,
        /** Catch-all for functions that don't fit other categories (CAST, CASE, COALESCE, EXTRACT, datetime, conversion). */
        SCALAR
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
        // TODO: Add an explicit functionName field per enum constant instead of relying on
        // valueOf(toUpperCase). This couples enum constant naming to SQL function naming convention.
        try {
            return ScalarFunction.valueOf(function.getName().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
