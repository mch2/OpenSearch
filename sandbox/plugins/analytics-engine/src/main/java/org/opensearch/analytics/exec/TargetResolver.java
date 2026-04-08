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
    public static List<PlanWalker.TargetShard> resolveTargets(
        Stage stage,
        ClusterService clusterService,
        Map<Integer, PlanWalker.StageOutput> stageOutputs
    ) {
        if (stage.getTableName() != null) {
            return resolveIndexShards(stage.getTableName(), clusterService);
        }
        if (stage.isShuffleWrite()) {
            return resolveShuffleTargets(stage, stageOutputs, clusterService);
        }
        return List.of();
    }

    static List<PlanWalker.TargetShard> resolveIndexShards(String tableName, ClusterService clusterService) {
        ClusterState state = clusterService.state();
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(state, new String[] { tableName }, null, null);

        List<PlanWalker.TargetShard> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = state.nodes().get(shard.currentNodeId());
                targets.add(new PlanWalker.TargetShard(shard.shardId(), node));
            }
        }
        return targets;
    }

    static List<PlanWalker.TargetShard> resolveShuffleTargets(
        Stage stage,
        Map<Integer, PlanWalker.StageOutput> stageOutputs,
        ClusterService clusterService
    ) {
        for (Stage child : stage.getChildStages()) {
            PlanWalker.StageOutput childOutput = stageOutputs.get(child.getStageId());
            if (childOutput instanceof PlanWalker.StageOutput.PartitionManifest manifest) {
                return pickShuffleTargetNodes(manifest, clusterService);
            }
        }
        throw new IllegalStateException("No partition manifest found for stage " + stage.getStageId());
    }

    static List<PlanWalker.TargetShard> pickShuffleTargetNodes(
        PlanWalker.StageOutput.PartitionManifest manifest,
        ClusterService clusterService
    ) {
        ClusterState state = clusterService.state();
        List<DiscoveryNode> sourceNodes = manifest.manifests()
            .keySet()
            .stream()
            .map(
                shardId -> state.nodes()
                    .get(state.routingTable().index(shardId.getIndex()).shard(shardId.id()).primaryShard().currentNodeId())
            )
            .distinct()
            .toList();

        int numPartitions = manifest.manifests().values().iterator().next().size();

        List<PlanWalker.TargetShard> targets = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            DiscoveryNode node = sourceNodes.get(p % sourceNodes.size());
            targets.add(new PlanWalker.TargetShard(new ShardId(new Index("_shuffle", "_na_"), p), node));
        }
        return targets;
    }
}
