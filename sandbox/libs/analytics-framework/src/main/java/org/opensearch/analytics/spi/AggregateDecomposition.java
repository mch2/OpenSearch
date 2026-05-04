/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * Declares the partial-state row type a backend's aggregate function emits
 * when run as a per-shard partial aggregate.
 *
 * <p>The split rule uses this to set the FINAL fragment's input row type
 * (and therefore the substrait {@code NamedScan.base_schema} that flows to
 * the coordinator). The same aggregate function name appears in both the
 * PARTIAL and FINAL fragments — DataFusion's {@code AggregateExec(Final)}
 * handles state merging internally via the function's accumulator
 * (forced via the native physical-plan mode rewriter on the coord side).
 *
 * <p>Defaults (no decomposition supplied) treat the state as identical to
 * the function's result type — correct for SUM/MIN/MAX/COUNT where the
 * single result column happens to also be a valid state column. Backends
 * override for functions whose state shape differs from their result:
 * <ul>
 *   <li>AVG → {@code [sum FLOAT8, count INT8]}</li>
 *   <li>STDDEV/VAR → {@code [count INT8, mean FLOAT8, m2 FLOAT8]} (Welford)</li>
 *   <li>HLL approx_distinct → {@code [sketch BINARY]}</li>
 *   <li>TDigest approx_percentile_cont → {@code [digest BINARY]}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface AggregateDecomposition {

    /**
     * The Calcite row type produced by this aggregate's partial stage. The
     * type is a struct whose fields become the partial output's columns
     * (after the optional groupBy fields). For a single-column state, return
     * a struct with one field; for multi-column state (e.g. AVG's sum/count),
     * return a struct with multiple fields.
     */
    RelDataType partialStateSchema(AggregateCall original, RelDataTypeFactory typeFactory);
}
