/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.segmentcopy.SegmentCopyService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportPublishShardCheckpointAction extends TransportReplicationAction<
    ShardPublishCheckpointRequest,
    ShardPublishCheckpointRequest,
    ReplicationResponse> {

    protected static Logger logger = LogManager.getLogger(TransportPublishShardCheckpointAction.class);

    public static final String ACTION_NAME = PublishCheckpointAction.NAME + "[s]";

    private final SegmentCopyService segmentCopyService;

    @Inject
    public TransportPublishShardCheckpointAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentCopyService segmentCopyService) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardPublishCheckpointRequest::new,
            ShardPublishCheckpointRequest::new,
            ThreadPool.Names.SNAPSHOT
        );
        this.segmentCopyService = segmentCopyService;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(ShardPublishCheckpointRequest shardRequest, IndexShard primary, ActionListener<PrimaryResult<ShardPublishCheckpointRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(shardRequest, new ReplicationResponse()));
    }

    @Override
    protected void shardOperationOnReplica(ShardPublishCheckpointRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            PublishCheckpointRequest request = shardRequest.getRequest();
            logger.info("Checkpoint received on replica [{}]", request);
            replica.onNewCheckpoint(request.getPrimaryGen(), request.getSegmentInfoVersion(), request.getSeqNo(), segmentCopyService);
            return new ReplicaResult();
        });
    }
}
