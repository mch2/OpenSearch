/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds the operator table that AE publishes to a front-end validator, derived from the
 * {@link ScalarFunction} / {@link AggregateFunction} / {@link WindowFunction} SPI enums —
 * the single source of truth for what functions AE recognizes.
 *
 * <p>The table exposes the concrete Calcite {@link SqlOperator} instances AE can resolve so a
 * front-end that parses SQL directly with Calcite validates AE functions during validation.
 * Not every enum entry maps to a standard Calcite operator: many AE-specific functions are
 * name-resolved user-defined functions ({@link org.apache.calcite.sql.SqlKind#OTHER_FUNCTION})
 * with no {@code SqlStdOperatorTable} constant. Those cannot be published by singleton identity,
 * so they are recorded in {@link #KNOWN_UNSUPPORTED_V1} rather than fabricated — a conscious,
 * test-guarded omission (see the inventory drift-guard test) rather than a silent gap.
 *
 * <p>Deriving from the enums means adding a new AE function is a single edit on the enum: it
 * either exposes a reference/standard operator (and is published automatically) or it does not
 * (and the inventory test forces it to be listed as known-unsupported).
 *
 * @opensearch.internal
 */
public final class AnalyticsOperatorTables {

    private AnalyticsOperatorTables() {}

    /**
     * Enum entries that carry no concrete, published Calcite operator — AE-specific UDFs resolved
     * by name at execution rather than by an operator-table lookup during validation. Kept as a
     * stable, inspected list so the front-end's function-inventory test can assert every enum
     * entry is either publishable or explicitly listed here, and so {@code AE-PACKAGING.md} can
     * document the v1 boundary against the same source.
     */
    public static final Set<String> KNOWN_UNSUPPORTED_V1;

    /**
     * Builds the operator table of every Calcite operator AE can resolve, derived from the SPI
     * enums. Chained ahead of {@code SqlStdOperatorTable.instance()} by the caller so std
     * operators still resolve; this table adds the AE-recognized operators that are not
     * identity-published by the std table on their own.
     */
    public static SqlOperatorTable build() {
        List<SqlOperator> operators = new ArrayList<>();

        // Publish ONLY the operators the chained SqlStdOperatorTable does not already provide:
        // AE scalars pinned to a concrete reference operator whose name is a non-identifier symbol
        // (e.g. CONCAT / "||", JSON_VALID / "IS JSON VALUE"). These need explicit publication because
        // a validator cannot reach them by identifier name.
        //
        // We deliberately do NOT re-add operators that ARE standard (SUM/MIN/MAX/COUNT/AVG and every
        // SqlKind-resolved scalar): they already resolve through the chained std table, and wrapping
        // a standard aggregate singleton in this ListSqlOperatorTable ahead of the std table shadows
        // its aggregate operand-type resolution ("No match found for function signature SUM(...)").
        for (ScalarFunction fn : ScalarFunction.values()) {
            SqlOperator op = fn.referenceOperator();
            if (op != null) {
                operators.add(op);
            }
        }

        return new ListSqlOperatorTable(operators);
    }

    static {
        Set<String> unsupported = new LinkedHashSet<>();
        for (ScalarFunction fn : ScalarFunction.values()) {
            if (isResolvable(fn) == false) {
                unsupported.add("scalar:" + fn.name());
            }
        }
        for (AggregateFunction fn : AggregateFunction.values()) {
            if (isResolvable(fn) == false) {
                unsupported.add("aggregate:" + fn.name());
            }
        }
        for (WindowFunction fn : WindowFunction.values()) {
            if (isResolvable(fn) == false) {
                unsupported.add("window:" + fn.name());
            }
        }
        KNOWN_UNSUPPORTED_V1 = Collections.unmodifiableSet(unsupported);
    }

    /** True for a SqlKind that identifies exactly one function (not the shared OTHER buckets). */
    private static boolean isConcreteKind(org.apache.calcite.sql.SqlKind kind) {
        return kind != org.apache.calcite.sql.SqlKind.OTHER && kind != org.apache.calcite.sql.SqlKind.OTHER_FUNCTION;
    }

    /**
     * Whether a validator can resolve this scalar by operator identity — either it pins a concrete
     * reference operator, or it carries a concrete {@link org.apache.calcite.sql.SqlKind} the chained
     * standard table resolves. A name-only {@code OTHER_FUNCTION} UDF is not validator-resolvable and
     * is therefore {@link #KNOWN_UNSUPPORTED_V1}. Public so the inventory drift-guard test partitions
     * the enums with this exact logic rather than duplicating it.
     */
    public static boolean isResolvable(ScalarFunction fn) {
        return fn.referenceOperator() != null || isConcreteKind(fn.getSqlKind());
    }

    /** Whether this aggregate maps to a concrete {@link SqlAggFunction} (else known-unsupported). */
    public static boolean isResolvable(AggregateFunction fn) {
        return tryAggOperator(fn) != null;
    }

    /** Whether this window function carries a concrete {@link org.apache.calcite.sql.SqlKind}. */
    public static boolean isResolvable(WindowFunction fn) {
        return isConcreteKind(fn.getSqlKind());
    }

    /** Returns the concrete SqlAggFunction for an aggregate, or null when it has none. */
    private static SqlAggFunction tryAggOperator(AggregateFunction fn) {
        try {
            return fn.toSqlAggFunction();
        } catch (IllegalStateException noMapping) {
            return null;
        }
    }
}
