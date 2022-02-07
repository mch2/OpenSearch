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

import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.indices.recovery.RecoveryRequestTracker;
import org.opensearch.indices.segmentcopy.CheckpointInfoResponse;
import org.opensearch.indices.segmentcopy.SegmentReplicationReplicaService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 *  Orchestrates a replication event for a replica shard.
 */
public class ReplicationTarget extends AbstractRefCounted {

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    private final ReplicationCheckpoint checkpoint;
    private static final AtomicLong idGenerator = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final long replicationId;
    private final IndexShard indexShard;
    private final Logger logger;
    private final ReplicationSource source;
    private final SegmentReplicationReplicaService.ReplicationListener listener;
    private final Store store;
    private final MultiFileWriter multiFileWriter;
    private final RecoveryRequestTracker requestTracker = new RecoveryRequestTracker();
    private final ReplicationState state;
    private volatile long lastAccessTime = System.nanoTime();

    private static final String REPLICATION_PREFIX = "replication.";

    /**
     * Creates a new replication target object that represents a replication to the provided source.
     *  @param indexShard local shard where we want to recover to
     * @param source     source node of the recovery where we recover from
     * @param listener   called when recovery is completed/failed
     */
    public ReplicationTarget(ReplicationCheckpoint checkpoint,IndexShard indexShard, ReplicationSource source, SegmentReplicationReplicaService.ReplicationListener listener) {
        super("replication_status");
        this.checkpoint = checkpoint;
        this.indexShard = indexShard;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.replicationId = idGenerator.incrementAndGet();
        this.source = source;
        this.listener = listener;
        this.store = indexShard.store();
        final String tempFilePrefix = REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
        this.multiFileWriter = new MultiFileWriter(
            indexShard.store(),
            indexShard.recoveryState().getIndex(),
            tempFilePrefix,
            logger,
            this::ensureRefCount
        );;
        // make sure the store is not released until we are done.
        store.incRef();
        state = new ReplicationState();
    }

    public void startReplication(ActionListener<ReplicationResponse> listener) {
        // Compute the current metadata snapshot for this shard.

        final StepListener<CheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetFilesResponse> getFilesListener = new StepListener<>();
        final StepListener<Void> finalizeListener = new StepListener<>();

        // Get list of files to copy from this checkpoint.
        source.getCheckpointInfo(replicationId, checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> getFiles(checkpointInfo, getFilesListener), listener::onFailure);
        getFilesListener.whenComplete(response -> finalizeRecovery(checkpoint, checkpointInfoListener.result(), finalizeListener), listener::onFailure);
        finalizeListener.whenComplete(r -> listener.onResponse(new ReplicationResponse()), listener::onFailure);
    }


    public void finalizeRecovery(ReplicationCheckpoint checkpoint, CheckpointInfoResponse checkpointInfo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.closeWriter();
            multiFileWriter.renameAllTempFiles();
            store.cleanupAndVerify("recovery CleanFilesRequestHandler", checkpointInfo.getSnapshot());

            for (String s : store.directory().listAll()) {
                logger.info("Files currently in store: {}", s);
            }
            indexShard.sync();
            indexShard.updateCurrentInfos(checkpoint.getSegmentsGen(), checkpointInfo.getInfosBytes());
            return null;
        });
    }

    public long getReplicationId() {
        return replicationId;
    }

    public IndexShard getIndexShard() {
        return indexShard;
    }

    public ReplicationSource getSource() {
        return source;
    }

    @Override
    protected void closeInternal() {
        store.decRef();
    }

    public ReplicationState state() {
        return state;
    }


    private void getFiles(CheckpointInfoResponse checkpointInfo, StepListener<GetFilesResponse> getFilesListener) {
        Store.MetadataSnapshot localMetadata = getMetadataSnapshot();
        final Store.RecoveryDiff diff = localMetadata.recoveryDiff(checkpointInfo.getSnapshot());
        final List<StoreFileMetadata> filesToFetch = Stream.concat(diff.missing.stream(), diff.different.stream())
            .collect(Collectors.toList());
        source.getFiles(replicationId, checkpoint, filesToFetch, getFilesListener);
    }

    private Store.MetadataSnapshot getMetadataSnapshot() {
        try {
            return indexShard.snapshotStoreMetadata();
        } catch (IOException e) {
            logger.warn("Unable to fetch current shard snapshot metadata, assume shard is empty", e);
            return Store.MetadataSnapshot.EMPTY;
        }
    }

    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        ActionListener<Void> listener
    ) {
        try {
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new OpenSearchException(
                "RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " + "calls"
            );
        }
    }

    /** mark the current recovery as done */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            try {
                // might need to do something on index shard here.
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onReplicationDone(state());
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(ReplicationFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onReplicationFailure(state(), e, sendShardFailure);
            } finally {
                try {
//                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
//                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }
}
