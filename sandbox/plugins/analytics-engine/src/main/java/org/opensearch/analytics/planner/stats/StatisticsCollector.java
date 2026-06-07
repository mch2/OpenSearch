/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-query entry point for index statistics consumed by the cost model.
 *
 * <p>Today this is a thin reader: each call walks {@link ClusterState#metadata()} to pull
 * {@link IndexMetadata#getNumberOfShards()} for the requested indices and returns
 * {@link TableStatistics} with {@code rowCount=0}. {@code clusterState.metadata().index(name)}
 * is an in-memory hash lookup, so the per-query cost is negligible — but {@code rowCount=0}
 * means downstream estimators fall back to {@code shards × defaultRowsPerShard}.
 *
 * <p>Future work plumbs an async {@code IndicesStatsAction}-backed cache behind this entry
 * point so {@code rowCount} returns real numbers without changing any caller. The cache will
 * be a separate component (TTL refresh + cluster-state listener), and this method's
 * signature stays the same — internally it consults the cache first, falls back to cluster
 * metadata for shard counts when the cache is cold or empty.
 *
 * @opensearch.internal
 */
public final class StatisticsCollector {

    private static final Logger LOGGER = LogManager.getLogger(StatisticsCollector.class);

    private StatisticsCollector() {}

    /**
     * Returns one {@link TableStatistics} per requested index. Indices missing from cluster
     * metadata are silently dropped (logged at WARN). Returns an empty map for an empty input.
     */
    public static Map<String, TableStatistics> collect(ClusterState clusterState, Collection<String> indexNames) {
        if (indexNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, TableStatistics> result = new HashMap<>(indexNames.size());
        for (String indexName : indexNames) {
            IndexMetadata metadata = clusterState.metadata().index(indexName);
            if (metadata == null) {
                LOGGER.warn("Index [{}] not found in cluster metadata — skipping statistics", indexName);
                continue;
            }
            // rowCount=0: callers fall back to shards × defaultRowsPerShard via
            // TableStatistics.rowCountOrEstimate. The cache wiring (future work) replaces this
            // with a real value.
            result.put(indexName, new TableStatistics(indexName, 0L, metadata.getNumberOfShards()));
        }
        return result;
    }
}
