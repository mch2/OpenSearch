/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.segmentcopy.copy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.segmentcopy.TransportCheckpointInfoResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 */
public class SegmentReplicationPrimaryService {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationPrimaryService.class);

    public static class Actions {
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/segrep/checkpoint_info";
        public static final String GET_FILES = "internal:index/shard/segrep/get_files";
    }

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;

    final CopyStateCache commitCache = new CopyStateCache();

    @Inject
    public SegmentReplicationPrimaryService(TransportService transportService, IndicesService indicesService, RecoverySettings recoverySettings) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        // When the target node wants to start a peer recovery it sends a START_RECOVERY request to the source
        // node. Upon receiving START_RECOVERY, the source node will initiate the peer recovery.
        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            StartReplicationRequest::new,
            new StartReplicationRequestHandler()
        );

        transportService.registerRequestHandler(
            Actions.GET_FILES,
            ThreadPool.Names.GENERIC,
            GetFilesRequest::new,
            new GetFilesRequestHandler()
        );
    }

    private static final class CopyStateCache {
        private final Map<ReplicationCheckpoint, NRTCopyState> checkpointCopyState = Collections.synchronizedMap(new HashMap<>());

        public void addCopyState(NRTCopyState copyState) {
            copyState.incRef();
            checkpointCopyState.putIfAbsent(copyState.getCheckpoint(), copyState);
        }

        public Optional<NRTCopyState> getCopyStateForCheckpoint(ReplicationCheckpoint checkpoint) {
            return Optional.of(checkpointCopyState.get(checkpoint));
        }

        public void removeCopyState(ReplicationCheckpoint checkpoint) {
            final Optional<NRTCopyState> nrtCopyState = Optional.ofNullable(checkpointCopyState.get(checkpoint));
            nrtCopyState.ifPresent((state) -> {
                state.decRef();
                if (state.refCount() <= 0) {
                    checkpointCopyState.remove(checkpoint);
                }
            });
        }
    }

    private class StartReplicationRequestHandler implements TransportRequestHandler<StartReplicationRequest> {
        @Override
        public void messageReceived(StartReplicationRequest request, TransportChannel channel, Task task) throws Exception {
            final ReplicationCheckpoint checkpoint = request.getCheckpoint();
            final ShardId shardId = checkpoint.getShardId();
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard shard = indexService.getShard(shardId.id());
            // If we don't have the requested checkpoint, create a new one from the latest commit on the shard.
            NRTCopyState nrtCopyState = commitCache.getCopyStateForCheckpoint(checkpoint).orElse(new NRTCopyState(shard));
            commitCache.addCopyState(nrtCopyState);
            channel.sendResponse(new TransportCheckpointInfoResponse(nrtCopyState.getCheckpoint(), nrtCopyState.getMetadataSnapshot(), nrtCopyState.getInfosBytes()));
        }
    }

    class GetFilesRequestHandler implements TransportRequestHandler<GetFilesRequest> {
        @Override
        public void messageReceived(GetFilesRequest request, TransportChannel channel, Task task) throws Exception {
            sendFiles(request, new ChannelActionListener<>(channel, Actions.GET_FILES, request));
        }
    }

    private void sendFiles(GetFilesRequest request, ActionListener<GetFilesResponse> listener) {
        final ShardId shardId = request.getCheckpoint().getShardId();
        final Optional<NRTCopyState> copyStateOptional = commitCache.getCopyStateForCheckpoint(request.getCheckpoint());
        copyStateOptional.ifPresent((copyState) -> {
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard shard = indexService.getShard(shardId.id());
            final ReplicaClient replicationTargetHandler = new ReplicaClient(
                shardId,
                transportService,
                request.getTargetNode(),
                recoverySettings,
                throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime)
            );
            SegmentReplicationSourceHandler handler = new SegmentReplicationSourceHandler(
                request.getReplicationId(),
                shard,
                replicationTargetHandler,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations());
            logger.trace(
                "[{}][{}] fetching files for {}",
                shardId.getIndex().getName(),
                shardId.id(),
                request.getTargetNode()
            );
            // TODO: The calling shard could die between requests without finishing - we need cancellation and a better way to refCount the indexCommits.
            handler.getFiles(copyState, ActionListener.runAfter(listener, () -> commitCache.removeCopyState(request.getCheckpoint())));
        });
    }
}
