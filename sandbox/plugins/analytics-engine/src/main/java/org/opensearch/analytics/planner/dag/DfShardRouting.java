/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.IndexResolution;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reshapes the same shard routing that {@link ShardTargetResolver} produces into what the Rust
 * {@code datafusion-distributed} coordinator needs (handed across FFM by
 * {@code DefaultPlanExecutor.executeInternalDistributed}): a shared, ordered list of hosting
 * {@link DiscoveryNode}s plus one {@link TableRouting} per distinct leaf table.
 *
 * <ul>
 *   <li><b>orderedWorkerNodes</b> — distinct data-node hosting {@link DiscoveryNode}s across ALL leaf
 *       tables, in stable order; the caller resolves each to a Worker gRPC URL (via
 *       {@code GetWorkerPort}) feeding {@code OsWorkerResolver.set_urls}.</li>
 *   <li><b>TableRouting.shardIds</b> — the REAL shard numbers ({@code ShardRouting.id()}) for that
 *       table, in task order; the data node resolves {@code getShard(shardId)}, so these are NOT
 *       dense ordinals.</li>
 *   <li><b>TableRouting.taskToWorkerUrlIndex</b> — index into {@code orderedWorkerNodes} for each
 *       shard, feeding {@code route_tasks} for shard affinity.</li>
 * </ul>
 *
 * <p>The DiscoveryNode → gRPC URL mapping is resolved by the caller after this routing is built: the
 * caller dials each node via the {@code GetWorkerPort} transport action to learn its bound (possibly
 * ephemeral) Worker port, then composes {@code "http://" + node.getHostAddress() + ':' + port}. (The
 * old {@code node.attr.datafusion_grpc_port} advertisement could not carry an ephemeral, post-bind
 * port and broke 2-nodes-per-host.)
 *
 * <p>Multi-table joins/unions route each leg to its own index's shards. Alias/wildcard leaves that
 * resolve to more than one concrete index per table are rejected (single concrete index per leaf).
 *
 * @opensearch.internal
 */
public final class DfShardRouting {

    /** Per-table shard routing: for each shard (task order) its real shard number + worker index. */
    public record TableRouting(String tableName, String indexUuid, int[] shardIds, int[] taskToWorkerUrlIndex) {}

    /**
     * The Rust-facing routing: the shared ordered worker-node list + one {@link TableRouting} per
     * distinct leaf table (join legs on different indices route independently).
     */
    public record Routing(List<DiscoveryNode> orderedWorkerNodes, List<TableRouting> tables) {
        /**
         * Newline-joined {@code table:shardId:workerIdx} lines — the wire form
         * {@code df_distributed_execute} parses into per-table {@code TableRouting}. {@code shardId}
         * is the REAL shard number (the data node resolves {@code getShard(shardId)}), NOT a dense
         * ordinal, so cross-index legs with different shard layouts route correctly.
         */
        public String shardMapCsv() {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (TableRouting t : tables) {
                for (int i = 0; i < t.shardIds.length; i++) {
                    if (first == false) {
                        sb.append('\n');
                    }
                    first = false;
                    sb.append(t.tableName).append(':').append(t.shardIds[i]).append(':').append(t.taskToWorkerUrlIndex[i]);
                }
            }
            return sb.toString();
        }

        /** Newline-joined {@code table=indexUuid} lines — the per-table index uuid map. */
        public String indexUuidCsv() {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (TableRouting t : tables) {
                if (first == false) {
                    sb.append('\n');
                }
                first = false;
                sb.append(t.tableName).append('=').append(t.indexUuid);
            }
            return sb.toString();
        }

        /** First table's index uuid (diagnostics / single-table callers). */
        public String firstIndexUuid() {
            return tables.isEmpty() ? "" : tables.getFirst().indexUuid;
        }
    }

    private DfShardRouting() {}

    /**
     * Builds per-table routing for EVERY distinct leaf table in the marked plan (join/union legs).
     * Each table's shards are resolved via {@code searchShards} over the SAME shared ordered
     * worker-node list, so a join between indices with different shard layouts routes each leg to its
     * own nodes. Shard ids are the REAL shard numbers ({@code ShardRouting.id()}) — the data node
     * resolves {@code getShard(shardId)}, not a dense ordinal.
     *
     * @param markedRoot the marked plan (one or more {@code OpenSearchTableScan} leaves)
     * @param clusterState planning cluster state
     * @param clusterService for {@code operationRouting().searchShards}
     * @param resolver index/alias expansion
     * @param maxShardsPerQuery snapshot of {@code analytics.query.max_shards_per_query}
     */
    public static Routing buildRouting(
        RelNode markedRoot,
        ClusterState clusterState,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver,
        int maxShardsPerQuery
    ) {
        // Distinct leaf tables in encounter order (a self-join references the same name once).
        List<OpenSearchTableScan> scans = RelNodeUtils.findAllNodes(markedRoot, OpenSearchTableScan.class);
        java.util.LinkedHashSet<String> tableNames = new java.util.LinkedHashSet<>();
        for (OpenSearchTableScan scan : scans) {
            tableNames.add(scan.getTable().getQualifiedName().getLast());
        }
        if (tableNames.isEmpty()) {
            throw new IllegalArgumentException("DfShardRouting: no OpenSearchTableScan found in plan");
        }

        // Shared worker-node list across all tables — a node hosting shards of multiple join legs
        // appears once, and each leg's taskToWorker indexes into this same list.
        List<DiscoveryNode> orderedWorkerNodes = new ArrayList<>();
        Map<String, Integer> nodeIdToUrlIndex = new LinkedHashMap<>();
        List<TableRouting> tables = new ArrayList<>(tableNames.size());
        int totalShards = 0;

        for (String tableName : tableNames) {
            IndexResolution resolution = IndexResolution.resolve(tableName, clusterState, resolver);
            String[] concreteNames = resolution.concreteIndexNames().toArray(new String[0]);
            GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
                .searchShards(clusterState, concreteNames, null, null);

            // Phase-1 constraint: one concrete index per leaf table (no alias/wildcard fan-out yet).
            if (resolution.concreteIndices().size() != 1) {
                throw new IllegalArgumentException(
                    "distributed_engine: table [" + tableName + "] resolves to " + resolution.concreteIndices().size()
                        + " concrete indices; only single-index leaves are supported on the distributed path"
                );
            }
            String indexUuid = resolution.concreteIndices().getFirst().getIndexUUID();

            List<Integer> shardIds = new ArrayList<>();
            List<Integer> taskToWorker = new ArrayList<>();
            for (ShardIterator shardIt : shardIterators) {
                ShardRouting shard = shardIt.nextOrNull();
                if (shard == null) {
                    continue;
                }
                DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                if (node == null) {
                    continue;
                }
                int urlIndex = nodeIdToUrlIndex.computeIfAbsent(node.getId(), id -> {
                    orderedWorkerNodes.add(node);
                    return orderedWorkerNodes.size() - 1;
                });
                shardIds.add(shard.id()); // REAL shard number — the data node resolves getShard(shardId)
                taskToWorker.add(urlIndex);
            }
            totalShards += shardIds.size();
            tables.add(
                new TableRouting(
                    tableName,
                    indexUuid,
                    shardIds.stream().mapToInt(Integer::intValue).toArray(),
                    taskToWorker.stream().mapToInt(Integer::intValue).toArray()
                )
            );
        }

        if (totalShards > maxShardsPerQuery) {
            throw new IllegalArgumentException(
                "Query targets ["
                    + totalShards
                    + "] shards which exceeds the limit of ["
                    + maxShardsPerQuery
                    + "] set by [analytics.query.max_shards_per_query]. Query an individual backing index directly."
            );
        }

        return new Routing(orderedWorkerNodes, tables);
    }
}
