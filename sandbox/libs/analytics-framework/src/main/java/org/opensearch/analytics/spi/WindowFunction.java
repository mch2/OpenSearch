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
 * Window functions a backend may support. Covers ranking functions (per-row, derived
 * from the OVER clause's ordering) and aggregate-as-window (SUM/AVG/COUNT/MIN/MAX
 * over a frame). PARTITION BY is not currently supported by the planner.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    ROW_NUMBER(SqlKind.ROW_NUMBER),
    RANK(SqlKind.RANK),
    DENSE_RANK(SqlKind.DENSE_RANK),
    SUM(SqlKind.SUM),
    AVG(SqlKind.AVG),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns the {@link WindowFunction} for {@code sqlKind}, or {@code null} if unsupported. */
    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }
}
