/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stateless utility for resolving target shards/nodes for a stage.
 * Extracted from {@link PlanWalker} for testability.
 *
 * @opensearch.internal
 */
public final class TargetResolver {

    private TargetResolver() {}

    /**
     * Resolve target shards/nodes based on the stage's properties.
     * Uses {@code stage.getTableName()} and {@code stage.isShuffleWrite()}
     * — no RelNode tree walking needed.
     */
    public static List<ShardTarget> resolveTargets(
        Stage stage,
        ClusterService clusterService,
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests
    ) {
        if (stage.getTableName() != null) {
            return resolveIndexShards(stage.getTableName(), clusterService);
        }
        if (stage.isShuffleWrite()) {
            return resolveShuffleTargets(stage, shuffleManifests, clusterService);
        }
        return List.of();
    }

    static List<ShardTarget> resolveIndexShards(String tableName, ClusterService clusterService) {
        ClusterState state = clusterService.state();
        // TODO: support routing/preference params?
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(state, new String[] { tableName }, null, null);

        List<ShardTarget> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = state.nodes().get(shard.currentNodeId());
                targets.add(new ShardTarget(shard.shardId(), node));
            }
        }
        return targets;
    }

    static List<ShardTarget> resolveShuffleTargets(
        Stage stage,
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests,
        ClusterService clusterService
    ) {
        for (Stage child : stage.getChildStages()) {
            Map<ShardId, Map<Integer, String>> manifest = shuffleManifests.get(child.getStageId());
            if (manifest != null) {
                return pickShuffleTargetNodes(manifest, clusterService);
            }
        }
        throw new IllegalStateException("No partition manifest found for stage " + stage.getStageId());
    }

    static List<ShardTarget> pickShuffleTargetNodes(Map<ShardId, Map<Integer, String>> manifest, ClusterService clusterService) {
        ClusterState state = clusterService.state();
        List<DiscoveryNode> sourceNodes = manifest.keySet()
            .stream()
            .map(
                shardId -> state.nodes()
                    .get(state.routingTable().index(shardId.getIndex()).shard(shardId.id()).primaryShard().currentNodeId())
            )
            .distinct()
            .toList();

        int numPartitions = manifest.values().iterator().next().size();

        List<ShardTarget> targets = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            DiscoveryNode node = sourceNodes.get(p % sourceNodes.size());
            targets.add(new ShardTarget(new ShardId(new Index("_shuffle", "_na_"), p), node));
        }
        return targets;
    }
}
