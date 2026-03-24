/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.delegation.DelegationTarget;
import org.opensearch.analytics.delegation.DelegationType;
import org.opensearch.index.engine.DataFormatAwareEngine;

import java.util.Set;


/**
 * SPI extension point for analytics query planning and execution.
 * <p>
 * Separate from {@code ReaderManagerProvider} which handles per-shard search
 * execution (readers, engines, filter providers). This interface is for
 * the analytics planning layer: bridge, operator tables, and capabilities.
 *
 * @opensearch.internal
 */
public interface AnalyticsSearchBackendPlugin {
    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /** Creates a searcher bound to the given reader snapshot. */
    SearchExecEngine searcher(ExecutionContext ctx, DataFormatAwareEngine.DataFormatAwareReader reader);

    /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
    SqlOperatorTable operatorTable();

    /** Returns the set of RelNode operator classes this backend supports. */
    default Set<Class<? extends RelNode>> supportedOperators() {
        return Set.of(
            LogicalTableScan.class,
            LogicalFilter.class,
            LogicalAggregate.class,
            LogicalProject.class
        );
    }

    /** Returns true if this backend can accept and execute the given opaque predicate payload. */
    default boolean canAcceptUnresolvedPredicate(byte[] payload) {
        return false;
    }

    /**
     * Returns a delegation target for the given type, built from the provided engine.
     * Returns null if this backend does not support the requested delegation type.
     *
     * @param type   the delegation type requested
     * @param engine the search engine holding reader/context state
     * @return a delegation target, or null if unsupported
     */
    default DelegationTarget getDelegationTarget(DelegationType type, SearchExecEngine engine) {
        return null;
    }
}
