/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal per-query index-statistics collector.
 *
 * <p>Pulls shard counts directly from {@link ClusterState} metadata. Row counts are omitted in
 * this version — a future pass will integrate {@code IndicesStatsResponse} for CBO-quality row
 * counts. With shard counts only, downstream cost computations fall back to
 * {@code shards × DEFAULT_ROWS_PER_SHARD} (see {@link TableStatistics#rowCountOrEstimate}).
 *
 * @opensearch.internal
 */
public final class StatisticsCollector {

    private static final Logger LOGGER = LogManager.getLogger(StatisticsCollector.class);

    private StatisticsCollector() {}

    /** Builds a {@code Map<indexName, TableStatistics>} from cluster metadata. */
    public static Map<String, TableStatistics> collect(ClusterState clusterState, Collection<String> indexNames) {
        Map<String, TableStatistics> result = new HashMap<>();
        for (String indexName : indexNames) {
            IndexMetadata metadata = clusterState.metadata().index(indexName);
            if (metadata == null) {
                LOGGER.warn("Index [{}] not found in cluster metadata — skipping statistics", indexName);
                continue;
            }
            // Row count defaults to 0 (unknown); callers fall back to a per-shard estimate.
            result.put(indexName, new TableStatistics(indexName, 0L, metadata.getNumberOfShards()));
        }
        return result;
    }
}
