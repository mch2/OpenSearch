/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AggregateDecomposition;

/**
 * Default {@link AggregateDecomposition} catalogue used by the split rule when
 * a backend doesn't supply its own per-function decomposition.
 *
 * <p>The default treats partial state as identical to the aggregate's result
 * type. That's correct for SUM/MIN/MAX/COUNT once the native side rewrites
 * the coord-side {@code AggregateExec} to {@code Final} mode — Final-mode
 * accumulators read state columns of the result-type-equivalent shape and
 * merge them via the function's own merge logic.
 *
 * <p>The split rule applies the default whenever {@code AggregateCapability.decomposition()}
 * returns null — i.e., the backend hasn't declared a non-default state shape.
 *
 * @opensearch.internal
 */
public final class StandardAggregateDecompositions {

    private StandardAggregateDecompositions() {}

    /** Default decomposition: state row type = struct with one field of the
     *  aggregate's result type. Used for SUM/MIN/MAX/COUNT where the partial
     *  output column shape matches the result. */
    public static final AggregateDecomposition DEFAULT = (original, typeFactory) -> typeFactory.createStructType(
        java.util.List.of(original.getType()),
        java.util.List.of(original.getName())
    );

    /** Returns the configured decomposition, or {@link #DEFAULT} if null. */
    public static AggregateDecomposition orDefault(AggregateDecomposition decomposition) {
        return decomposition != null ? decomposition : DEFAULT;
    }

    // ── Helpers for backends that ship state-shape declarations ─────────────────

    /** Builds a struct row type from a list of (name, SqlTypeName) pairs. Convenience
     *  for backends declaring decompositions like AVG → {@code [sum DOUBLE, count BIGINT]}. */
    public static RelDataType structOf(RelDataTypeFactory typeFactory, java.util.List<Field> fields) {
        java.util.List<RelDataType> types = new java.util.ArrayList<>(fields.size());
        java.util.List<String> names = new java.util.ArrayList<>(fields.size());
        for (Field f : fields) {
            RelDataType base = typeFactory.createSqlType(f.type());
            types.add(typeFactory.createTypeWithNullability(base, f.nullable()));
            names.add(f.name());
        }
        return typeFactory.createStructType(types, names);
    }

    /** A field declaration for {@link #structOf(RelDataTypeFactory, java.util.List)}. */
    public record Field(String name, SqlTypeName type, boolean nullable) {
    }
}
