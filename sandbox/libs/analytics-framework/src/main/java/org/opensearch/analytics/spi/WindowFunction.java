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
 * Window functions a backend may support inside a {@code RexOver} projection expression.
 *
 * <p>Covers pure ranking/navigation window functions (ROW_NUMBER, RANK, LEAD, …) as well
 * as aggregate functions that can be invoked through an {@code OVER (…)} clause (SUM, COUNT,
 * …). The aggregate overlap mirrors isthmus's {@code FunctionMappings.WINDOW_SIGS} which
 * unions {@code AGGREGATE_SIGS} for Substrait function-name resolution on the conversion
 * side.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    // Ranking / navigation
    ROW_NUMBER(SqlKind.ROW_NUMBER),
    RANK(SqlKind.RANK),
    DENSE_RANK(SqlKind.DENSE_RANK),
    LEAD(SqlKind.LEAD),
    LAG(SqlKind.LAG),
    NTILE(SqlKind.NTILE),

    // Aggregates usable as window functions
    SUM(SqlKind.SUM),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    AVG(SqlKind.AVG);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Maps a Calcite {@link SqlKind} (taken from a {@code RexOver}'s operator) to a {@link WindowFunction}, or null if unrecognized. */
    public static WindowFunction fromSqlKind(SqlKind kind) {
        for (WindowFunction func : values()) {
            if (func.sqlKind == kind) {
                return func;
            }
        }
        return null;
    }
}
