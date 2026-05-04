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
 * Aggregate functions that a backend may support, categorized by {@link Type}.
 *
 * <p>Note: {@code COUNT(DISTINCT x)} is rewritten to {@code APPROX_COUNT_DISTINCT(x)}
 * by {@code OpenSearchAggregateRule} — PPL's dc/distinct_count semantics are HLL++
 * approximate. Plain {@code COUNT} covers {@code COUNT(*)} and {@code COUNT(x)}.
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

    // Simple — first/last value (no time semantics; DF first_value/last_value)
    FIRST(Type.SIMPLE, SqlKind.OTHER),
    LAST(Type.SIMPLE, SqlKind.OTHER),
    FIRST_VALUE(Type.SIMPLE, SqlKind.FIRST_VALUE),
    LAST_VALUE(Type.SIMPLE, SqlKind.LAST_VALUE),

    // Time-ordered (PPL `stats earliest(field, ts)` / `latest(field, ts)`).
    // Calcite emits these as ARG_MIN/ARG_MAX; DataFusion's name is min_by/max_by
    // (handled by NAME_ALIASES in NameBasedAggregateFunctionConverter).
    ARG_MIN(Type.SIMPLE, SqlKind.ARG_MIN),
    ARG_MAX(Type.SIMPLE, SqlKind.ARG_MAX),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    PERCENTILE(Type.STATE_EXPANDING, SqlKind.OTHER),
    MEDIAN(Type.STATE_EXPANDING, SqlKind.OTHER),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    VALUES(Type.STATE_EXPANDING, SqlKind.OTHER),
    LIST(Type.STATE_EXPANDING, SqlKind.OTHER),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),
    TAKE(Type.STATE_EXPANDING, SqlKind.OTHER),

    // Approximate — probabilistic, fixed-size state
    APPROX_COUNT_DISTINCT(Type.APPROXIMATE, SqlKind.OTHER),
    DISTINCT_COUNT(Type.APPROXIMATE, SqlKind.OTHER),
    DC(Type.APPROXIMATE, SqlKind.OTHER),
    PERCENTILE_APPROX(Type.STATE_EXPANDING, SqlKind.OTHER),
    APPROX_MEDIAN(Type.APPROXIMATE, SqlKind.OTHER);

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

    /** Resolves the {@code AggregateFunction} for a Calcite {@code AggregateCall}.
     *  Tries name-based lookup first (handles cases like APPROX_COUNT_DISTINCT where
     *  the Calcite function name is more specific than SqlKind.COUNT), then falls
     *  back to SqlKind-based lookup. */
    public static AggregateFunction resolve(org.apache.calcite.rel.core.AggregateCall call) {
        AggregateFunction byName = fromName(call.getAggregation().getName());
        if (byName != null) {
            return byName;
        }
        AggregateFunction byKind = fromSqlKind(call.getAggregation().getKind());
        if (byKind != null) {
            return byKind;
        }
        throw new IllegalStateException("Unrecognized aggregate function [" + call.getAggregation().getName() + "]");
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

    /** Maps an aggregate function name to an AggregateFunction, or null if not recognized.
     *  Case-insensitive. */
    public static AggregateFunction fromName(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
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
