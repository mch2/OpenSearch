/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

/**
 * Aggregate functions that a backend may support, categorized by {@link Type}.
 *
 * <p>Note: {@code COUNT} covers both {@code COUNT(*)} and {@code COUNT(DISTINCT x)}.
 * The distinction is on {@code AggregateCall.isDistinct()}, not on SqlKind.
 *
 * @opensearch.internal
 */
public enum AggregateFunction {
    // Simple — fixed-size state per key
    SUM(Type.SIMPLE, SqlKind.SUM),
    SUM0(Type.SIMPLE, SqlKind.SUM0),
    MIN(Type.SIMPLE, SqlKind.MIN),
    MAX(Type.SIMPLE, SqlKind.MAX),
    COUNT(Type.SIMPLE, SqlKind.COUNT),
    AVG(Type.SIMPLE, SqlKind.AVG),

    // Statistical — fixed-size state, multi-pass or running stats
    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    // Simple — first/last value semantics. PPL emits SqlAggFunction named "first" /
    // "last"; backend-side ADDITIONAL_AGG_SIGS maps those to DataFusion's
    // "first_value"/"last_value" before substrait emission.
    FIRST(Type.SIMPLE, SqlKind.OTHER),
    LAST(Type.SIMPLE, SqlKind.OTHER),

    // PPL earliest(field, ts) / latest(field, ts) lower to Calcite ARG_MIN / ARG_MAX
    // at the frontend. Backends rewrite ARG_MIN(x, ts) → first_value(x) ORDER BY ts ASC
    // (and ARG_MAX → last_value) via BackendCapabilityProvider#aggregateCallAdapters().
    // DataFusion 52.x has no native min_by/max_by UDAF.
    ARG_MIN(Type.SIMPLE, SqlKind.ARG_MIN),
    ARG_MAX(Type.SIMPLE, SqlKind.ARG_MAX),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),
    TAKE(Type.STATE_EXPANDING, SqlKind.OTHER),
    // PPL list(field) is a pure rename to DataFusion's native array_agg, wired
    // via ADDITIONAL_AGG_SIGS on the backend side. VALUES additionally needs
    // DISTINCT + ORDER BY on the operand itself — handled in the backend's
    // aggregate function converter.
    LIST(Type.STATE_EXPANDING, SqlKind.OTHER),
    VALUES(Type.STATE_EXPANDING, SqlKind.OTHER),

    // Approximate — probabilistic, fixed-size state
    APPROX_COUNT_DISTINCT(Type.APPROXIMATE, SqlKind.OTHER);

    /** Category of aggregate function. Affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    private final Type type;
    private final SqlKind sqlKind;

    AggregateFunction(Type type, SqlKind sqlKind) {
        this.type = type;
        this.sqlKind = sqlKind;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Maps a Calcite SqlKind to an AggregateFunction, or null if not recognized. Skips OTHER. */
    public static AggregateFunction fromSqlKind(SqlKind kind) {
        for (AggregateFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /**
     * Resolves the {@link AggregateFunction} for an {@link AggregateCall}. Prefers {@link SqlKind}
     * dispatch (handles the COUNT vs APPROX_COUNT_DISTINCT ambiguity via {@code isApproximate})
     * and falls back to case-insensitive name match for PPL operators that emit {@code SqlKind.OTHER}.
     * Returns {@code null} if unrecognized.
     */
    public static AggregateFunction fromAggregateCall(AggregateCall call) {
        if (call.getAggregation().getKind() == SqlKind.COUNT && call.isApproximate()) {
            return APPROX_COUNT_DISTINCT;
        }
        AggregateFunction func = fromSqlKind(call.getAggregation().getKind());
        if (func != null) {
            return func;
        }
        try {
            return fromNameOrError(call.getAggregation().getName());
        } catch (IllegalStateException ignored) {
            return null;
        }
    }

    /** Maps an aggregate function name to an AggregateFunction. Throws if not recognized.
     *  Lookup is case-insensitive — Calcite SqlAggFunction names are lowercase
     *  while enum constants follow Java convention (uppercase). */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }
}
