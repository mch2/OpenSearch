/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationStateResponse;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Action to get a realtime view of seqNo state for all replicas in a group.
 * This action also allows you to wait on a specific seqNo
 *
 * @opensearch.internal
 */

public class TransportSegmentReplicationStateAction {

    protected static Logger logger = LogManager.getLogger(TransportSegmentReplicationStateAction.class);
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public TransportSegmentReplicationStateAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    public void execute(SegmentReplicationStateRequest req, ActionListener<Void> listener) {
        ShardId shardId = req.shardId();
        ShardIterator shards = clusterService.operationRouting().getShards(clusterService.state(), shardId.getIndexName(), shardId.id(), Preference.REPLICA.name());
        List<ShardRouting> shardRoutings = shards.getShardRoutings().stream().filter(r -> r.primary() == false).collect(Collectors.toList());
        if (shardRoutings.isEmpty()) {
            listener.onResponse(null);
        } else {
            GroupedActionListener<Void> grouped = new GroupedActionListener<>(ActionListener.map(listener, v -> null), shardRoutings.size());
            ActionListener<SegmentReplicationStateResponse> l = ActionListener.map(grouped, r -> null);
            shardRoutings.forEach(r -> waitForReplica(r, req, l));
        }
    }

    private void waitForReplica(ShardRouting routing, SegmentReplicationStateRequest req, ActionListener<SegmentReplicationStateResponse> listener) {
        assert routing.primary() == false;
        final DiscoveryNode node = clusterService.state().nodes().get(routing.currentNodeId());
        // TODO: use retryable client
        transportService.sendRequest(node, SegmentReplicationTargetService.Actions.WAIT_FOR_SEQ_NO, req, new ActionListenerResponseHandler<>(listener, SegmentReplicationStateResponse::new, ThreadPool.Names.GENERIC));
    }

}
