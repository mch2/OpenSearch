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
    IN(Category.COMPARISON, SqlKind.IN),
    LIKE(Category.COMPARISON, SqlKind.LIKE),
    PREFIX(Category.COMPARISON, SqlKind.OTHER_FUNCTION),
    /** Calcite's Sarg fold for IN / NOT IN / BETWEEN / range-union. Backends expand it before substrait. */
    SARG_PREDICATE(Category.SCALAR, SqlKind.SEARCH),

    // ── Full-text search ─────────────────────────────────────────────
    MATCH(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    MATCH_PHRASE(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    FUZZY(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    WILDCARD(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),
    REGEXP(Category.FULL_TEXT, SqlKind.OTHER_FUNCTION),

    // ── String ───────────────────────────────────────────────────────
    UPPER(Category.STRING, SqlKind.OTHER_FUNCTION),
    LOWER(Category.STRING, SqlKind.OTHER_FUNCTION),
    TRIM(Category.STRING, SqlKind.TRIM),
    SUBSTRING(Category.STRING, SqlKind.OTHER_FUNCTION),
    CONCAT(Category.STRING, SqlKind.OTHER_FUNCTION),
    CHAR_LENGTH(Category.STRING, SqlKind.OTHER_FUNCTION),

    // ── Math ─────────────────────────────────────────────────────────
    PLUS(Category.MATH, SqlKind.PLUS),
    MINUS(Category.MATH, SqlKind.MINUS),
    TIMES(Category.MATH, SqlKind.TIMES),
    DIVIDE(Category.MATH, SqlKind.DIVIDE),
    MOD(Category.MATH, SqlKind.MOD),
    ABS(Category.MATH, SqlKind.OTHER_FUNCTION),
    SIN(Category.MATH, SqlKind.OTHER_FUNCTION),
    CEIL(Category.MATH, SqlKind.CEIL),
    FLOOR(Category.MATH, SqlKind.FLOOR),

    // ── Cast / type ──────────────────────────────────────────────────
    CAST(Category.SCALAR, SqlKind.CAST),

    // ── Conditional ──────────────────────────────────────────────────
    CASE(Category.SCALAR, SqlKind.CASE),
    COALESCE(Category.SCALAR, SqlKind.COALESCE),
    NULLIF(Category.SCALAR, SqlKind.NULLIF),

    EXTRACT(Category.SCALAR, SqlKind.EXTRACT),

    // ── Datetime ────────────────────────────────────────────────────
    TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    YEAR(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    CONVERT_TZ(Category.SCALAR, SqlKind.OTHER_FUNCTION),
    /** PPL unix_timestamp(ts). Resolved to DataFusion's native to_unixtime. */
    UNIX_TIMESTAMP(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    // ── Bucketing (PPL-specific; return VARCHAR bucket labels) ──────
    /** PPL span_bucket(value, span). Resolved to the span_bucket Rust UDF.
     *  NOT ISO SQL width_bucket — returns a VARCHAR bucket label like "10-20",
     *  not an integer bucket index. */
    SPAN_BUCKET(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    /** PPL width_bucket(value, num_bins, data_range, max_value). Resolved to
     *  the width_bucket Rust UDF via {@code WidthBucketAdapter}. Returns a
     *  VARCHAR bucket label via the OpenSearch nice-number algorithm — name
     *  collides with ISO-SQL WIDTH_BUCKET (bucket *index*) but semantics and
     *  signature both differ.
     *
     *  <p>TODO(future): PPL WIDTH_BUCKET collides with ISO SQL width_bucket
     *  (different semantics — bucket label string vs bucket index int). Real
     *  SQL width_bucket will need a distinct enum entry or namespace (e.g.
     *  {@code SQL_WIDTH_BUCKET}, or a catalog-qualified name) so dispatch
     *  can route each to its correct implementation. DataFusion has
     *  ISO-style width_bucket natively, so the future entry would be cat 1
     *  or cat 2, not cat 4. */
    WIDTH_BUCKET(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    /** PPL minspan_bucket(value, min_span, data_range, max_value). Resolved to
     *  the minspan_bucket Rust UDF via {@code MinspanBucketAdapter}. Returns a
     *  VARCHAR bucket label. The 4-arg shape mirrors width_bucket but the
     *  algorithm uses a magnitude-based width selection against a user min_span
     *  floor; {@code max_value} is carried through for parity and ignored. */
    MINSPAN_BUCKET(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    /** PPL span(value, interval, unit). Resolved to the span Rust UDF via
     *  {@code SpanAdapter} for the numeric-span case (unit = null). Date/time
     *  spans (unit = "d"/"h"/...) are bridged on the coordinator — this data-
     *  node UDF returns a plan error if a non-null unit reaches it. */
    SPAN(Category.SCALAR, SqlKind.OTHER_FUNCTION),

    /** PPL range_bucket(value, data_min, data_max, start_param, end_param).
     *  Resolved to the range_bucket Rust UDF via {@code RangeBucketAdapter}.
     *  Expansion-only effective bounds: null start/end slots are sentinels
     *  meaning "use the observed data bound" (do not null-propagate).
     *  End-to-end pushdown via PPL's {@code bin start=... end=...} command
     *  additionally requires window-over-empty-partition backend support. */
    RANGE_BUCKET(Category.SCALAR, SqlKind.OTHER_FUNCTION);

    /**
     * Category of scalar function.
     */
    public enum Category {
        COMPARISON,
        FULL_TEXT,
        STRING,
        MATH,
        /**
         * Catch-all for functions that don't fit other categories (CAST, CASE, COALESCE, EXTRACT, etc.).
         */
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
     * Skips OTHER_FUNCTION — multiple functions share this kind,
     * so they must be resolved by name via {@link #fromSqlFunction(SqlFunction)}.
     */
    public static ScalarFunction fromSqlKind(SqlKind kind) {
        for (ScalarFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER_FUNCTION) {
                return func;
            }
        }
        return null;
    }

    /**
     * Maps a Calcite SqlFunction to a ScalarFunction by name, or null if not recognized.
     */
    public static ScalarFunction fromSqlFunction(SqlFunction function) {
        // TODO: Add an explicit functionName field per enum constant instead of relying on
        // valueOf(toUpperCase). This couples enum constant naming to SQL function naming convention.
        try {
            return ScalarFunction.valueOf(function.getName().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            // Callers (e.g. OpenSearchProjectRule) short-circuit on null — routing the
            // function through the non-ScalarFunction path (opaque or YAML-alias based
            // name lookup) rather than aborting Hep rule matching with an exception.
            return null;
        }
    }
}
