/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.PeerRecoveryTargetService.RecoveryListener;
import org.opensearch.indices.recovery.RecoveryFailedException;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.segmentcopy.copy.SegmentReplicationTargetService;

public class PrimarySegmentCopy implements SegmentCopyService {

    private static final Logger logger = LogManager.getLogger(PrimarySegmentCopy.class);

    private final ClusterService clusterService;
    private final SegmentReplicationTargetService replicationTargetService;

    @Inject
    public PrimarySegmentCopy(ClusterService clusterService,
                              SegmentReplicationTargetService replicationTargetService) {
        this.clusterService = clusterService;
        this.replicationTargetService = replicationTargetService;
    }

    public void pullSegments(long primaryTerm, long segmentInfosVersion, long seqNo, IndexShard shard) {
        ClusterState state = clusterService.state();
        ShardRouting primaryShard = state.routingTable().shardRoutingTable(shard.shardId()).primaryShard();
        DiscoveryNode primaryNode = state.nodes().get(primaryShard.currentNodeId());
        logger.info("Initiating recovery from PrimarySegmentCopy. ShardRoutingState {}", shard.routingEntry().state());
        replicationTargetService.startRecovery(shard, primaryNode, new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state) {
                logger.info("Done.");
            }

            @Override
            public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
                logger.error("Failure", e);
            }
        });
    }
}
