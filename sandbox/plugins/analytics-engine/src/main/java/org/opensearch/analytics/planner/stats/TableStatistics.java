/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.stats;

/**
 * Per-index statistics used by the cost model for plan-shape decisions and (eventually)
 * by {@code JoinStrategySelector} to pick a join strategy.
 *
 * <p>{@code rowCount} is 0 when statistics are unavailable. Callers should fall back to
 * {@code shardCount × DEFAULT_ROWS_PER_SHARD} (or another conservative default) in that case
 * — a 0-row index is rare; a 0-row stat almost always means "not yet populated".
 *
 * <p>{@code shardCount} is always non-zero (derived from {@code ClusterState} metadata).
 *
 * @opensearch.internal
 */
public record TableStatistics(String indexName, long rowCount, int shardCount) {
    /**
     * Returns the best-effort row count: real stat when present, otherwise
     * {@code shardCount × defaultRowsPerShard}. The default is a conservative placeholder
     * for analytics-class indexes — large enough that downstream cost arithmetic
     * differentiates plan alternatives.
     */
    public double rowCountOrEstimate(double defaultRowsPerShard) {
        if (rowCount > 0) {
            return rowCount;
        }
        return (long) shardCount * (long) defaultRowsPerShard;
    }
}
