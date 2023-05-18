/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardNotStartedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages references to ongoing segrep events on a node.
 * Each replica will have a new {@link SegmentReplicationSourceHandler} created when starting replication.
 * CopyStates will be cached for reuse between replicas and only released when all replicas have finished copying segments.
 *
 * @opensearch.internal
 */
class OngoingSegmentReplications {

    private static final Logger logger = LogManager.getLogger(OngoingSegmentReplications.class);
    private final SegmentReplicationSettings segmentReplicationSettings;
    private final IndicesService indicesService;
    private final Map<ShardId, CopyState> copyStateMap;
    private final Map<String, SegmentReplicationSourceHandler> allocationIdToHandlers;

    /**
     * Constructor.
     *
     * @param indicesService   {@link IndicesService}
     * @param recoverySettings {@link RecoverySettings}
     */
    OngoingSegmentReplications(IndicesService indicesService, SegmentReplicationSettings recoverySettings) {
        this.indicesService = indicesService;
        this.segmentReplicationSettings = recoverySettings;
        this.copyStateMap = ConcurrentCollections.newConcurrentMap();
        this.allocationIdToHandlers = ConcurrentCollections.newConcurrentMap();
    }

    /**
     * Start sending files to the replica.
     *
     * @param request  {@link GetSegmentFilesRequest}
     * @param listener {@link ActionListener} that resolves when sending files is complete.
     */
    void startSegmentCopy(GetSegmentFilesRequest request, RetryableTransportClient client, ActionListener<GetSegmentFilesResponse> listener) {
        final RemoteSegmentFileChunkWriter segmentSegmentFileChunkWriter = new RemoteSegmentFileChunkWriter(
            request.getReplicationId(),
            segmentReplicationSettings,
            client,
            request.getCheckpoint().getShardId(),
            SegmentReplicationTargetService.Actions.FILE_CHUNK,
            new AtomicLong(0),
            (throttleTime) -> {
            }
        );
        final GatedCloseable<CopyState> copyStateGatedCloseable = getCopyState(request.getCheckpoint().getShardId());
        final CopyState copyState = copyStateGatedCloseable.get();
        final SegmentReplicationSourceHandler handler = createTargetHandler(request.getTargetNode(), copyState, request.getTargetAllocationId(), segmentSegmentFileChunkWriter);
        final SegmentReplicationSourceHandler segmentReplicationSourceHandler = allocationIdToHandlers.putIfAbsent(
            request.getTargetAllocationId(),
            handler
        );
        if (segmentReplicationSourceHandler != null) {
            logger.error("Shard is already replicating, id {}", request.getReplicationId());
            throw new OpenSearchException(
                "Shard copy {} on node {} already replicating to {} rejecting id {}",
                request.getCheckpoint().getShardId(),
                request.getTargetNode(),
                segmentReplicationSourceHandler.getCopyState().getCheckpoint(),
                request.getReplicationId()
            );
        }
        final Store.RecoveryDiff recoveryDiff = Store.segmentReplicationDiff(copyState.getMetadataMap(), request.getMetadataMap());
        // update the given listener to release the CopyState before it resolves.
        final ActionListener<GetSegmentFilesResponse> wrappedListener = ActionListener.runBefore(listener, () -> {
            final SegmentReplicationSourceHandler sourceHandler = allocationIdToHandlers.remove(request.getTargetAllocationId());
//                if (sourceHandler != null) {
//                    removeCopyState(sourceHandler.getCopyState());
//                }
            copyStateGatedCloseable.close();
        });
        if (recoveryDiff.missing.isEmpty()) {
            // before completion, alert the primary of the replica's state.
            handler.getCopyState()
                .getShard()
                .updateVisibleCheckpointForShard(request.getTargetAllocationId(), handler.getCopyState().getCheckpoint(), 0L);
            try {
                copyStateGatedCloseable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            wrappedListener.onResponse(new GetSegmentFilesResponse(Collections.emptyList(), copyState.getCheckpoint(), copyState.getInfosBytes()));
        } else {
            handler.sendFiles(request, recoveryDiff.missing, wrappedListener);
        }
//    } else
//
//    {
//        listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList(), copyState.getCheckpoint(), copyState.getInfosBytes()));
//    }

}

    /**
     * Prepare for a Replication event. This method constructs a {@link CopyState} holding files to be sent off of the current
     * node's store.  This state is intended to be sent back to Replicas before copy is initiated so the replica can perform a diff against its
     * local store.  It will then build a handler to orchestrate the segment copy that will be stored locally and started on a subsequent request from replicas
     * with the list of required files.
     *
     * @param request         {@link CheckpointInfoRequest}
     * @param fileChunkWriter {@link FileChunkWriter} writer to handle sending files over the transport layer.
     */
    void prepareForReplication(GetSegmentFilesRequest request, FileChunkWriter fileChunkWriter, CopyState copyState) {
        final SegmentReplicationSourceHandler segmentReplicationSourceHandler = allocationIdToHandlers.putIfAbsent(
            request.getTargetAllocationId(),
            createTargetHandler(request.getTargetNode(), copyState, request.getTargetAllocationId(), fileChunkWriter)
        );
        if (segmentReplicationSourceHandler != null) {
            logger.error("Shard is already replicating, id {}", request.getReplicationId());
            throw new OpenSearchException(
                "Shard copy {} on node {} already replicating to {} rejecting id {}",
                request.getCheckpoint().getShardId(),
                request.getTargetNode(),
                segmentReplicationSourceHandler.getCopyState().getCheckpoint(),
                request.getReplicationId()
            );
        }
    }

    public GatedCloseable<CopyState> getCopyState(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        final CopyState copyState = indexShard.getCopyState();
        if (copyState != null) {
            copyState.incRef();
            return new GatedCloseable<>(copyState, copyState::decRef);
        }
        throw new IndexShardNotStartedException(shardId, indexShard.state());
    }

    /**
     * Cancel all Replication events for the given shard, intended to be called when a primary is shutting down.
     *
     * @param shard  {@link IndexShard}
     * @param reason {@link String} - Reason for the cancel
     */
    synchronized void cancel(IndexShard shard, String reason) {
        cancelHandlers(handler -> handler.getCopyState().getShard().shardId().equals(shard.shardId()), reason);
    }

    /**
     * Cancel all Replication events for the given allocation ID, intended to be called when a primary is shutting down.
     *
     * @param allocationId {@link String} - Allocation ID.
     * @param reason       {@link String} - Reason for the cancel
     */
    synchronized void cancel(String allocationId, String reason) {
        final SegmentReplicationSourceHandler handler = allocationIdToHandlers.remove(allocationId);
        if (handler != null) {
            handler.cancel(reason);
//            removeCopyState(handler.getCopyState());
        }
    }

    /**
     * Cancel any ongoing replications for a given {@link DiscoveryNode}
     *
     * @param node {@link DiscoveryNode} node for which to cancel replication events.
     */
    void cancelReplication(DiscoveryNode node) {
        cancelHandlers(handler -> handler.getTargetNode().equals(node), "Node left");
    }

    /**
     * Checks if the {@link #copyStateMap} has the input {@link ReplicationCheckpoint}
     * as a key by invoking {@link Map#containsKey(Object)}.
     */
    boolean isInCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.containsKey(replicationCheckpoint);
    }

    int size() {
        return allocationIdToHandlers.size();
    }

    // Visible for tests.
    Map<String, SegmentReplicationSourceHandler> getHandlers() {
        return allocationIdToHandlers;
    }

    int cachedCopyStateSize() {
        return copyStateMap.size();
    }

    private SegmentReplicationSourceHandler createTargetHandler(
        DiscoveryNode node,
        CopyState copyState,
        String allocationId,
        FileChunkWriter fileChunkWriter
    ) {
        return new SegmentReplicationSourceHandler(
            node,
            fileChunkWriter,
            copyState.getShard().getThreadPool(),
            copyState,
            allocationId,
            segmentReplicationSettings
        );
    }

    /**
     * Remove a CopyState. Intended to be called after a replication event completes.
     * This method will remove a copyState from the copyStateMap only if its refCount hits 0.
     *
     * @param copyState {@link CopyState}
     */
//    private void removeCopyState(CopyState copyState) {
//        if (copyState.decRef() == true) {
//            copyStateMap.remove(copyState.getRequestedReplicationCheckpoint());
//        }
//    }

    /**
     * Remove handlers from allocationIdToHandlers map based on a filter predicate.
     * This will also decref the handler's CopyState reference.
     */
    private void cancelHandlers(Predicate<? super SegmentReplicationSourceHandler> predicate, String reason) {
        final List<String> allocationIds = allocationIdToHandlers.values()
            .stream()
            .filter(predicate)
            .map(SegmentReplicationSourceHandler::getAllocationId)
            .collect(Collectors.toList());
        if (allocationIds.size() == 0) {
            return;
        }
        logger.warn(() -> new ParameterizedMessage("Cancelling replications for allocationIds {}", allocationIds));
        for (String allocationId : allocationIds) {
            cancel(allocationId, reason);
        }
    }

    /**
     * Clear copystate and target handlers for any non insync allocationIds.
     *
     * @param shardId             {@link ShardId}
     * @param inSyncAllocationIds {@link List} of in-sync allocation Ids.
     */
    public void clearOutOfSyncIds(ShardId shardId, Set<String> inSyncAllocationIds) {
        cancelHandlers(
            (handler) -> handler.getCopyState().getShard().shardId().equals(shardId)
                && inSyncAllocationIds.contains(handler.getAllocationId()) == false,
            "Shard is no longer in-sync with the primary"
        );
    }
}
