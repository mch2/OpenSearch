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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.ExceptionsHelper;
import org.opensearch.LegacyESVersion;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.StopWatch;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.IndexShardRelocatedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.*;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static org.opensearch.indices.recovery.RecoveryState.Translog.UNKNOWN;

/**
 * SegmentReplicationSourceHandler handles the three phases of shard recovery, which is
 * everything relating to copying the segment files as well as sending translog
 * operations across the wire once the segments have been copied.
 * <p>
 * Note: There is always one source handler per recovery that handles all the
 * file and translog transfer. This handler is completely isolated from other recoveries
 * while the {@link RateLimiter} passed via {@link RecoverySettings} is shared across recoveries
 * originating from this nodes to throttle the number bytes send during file transfer. The transaction log
 * phase bypasses the rate limiter entirely.
 */
public class SegmentReplicationSourceHandler {

    protected final Logger logger;
    // Shard that is going to be recovered (the "source")
    private final IndexShard shard;
    private final int shardId;
    // Request containing source and target node information
    private final StartRecoveryRequest request;
    private final int chunkSizeInBytes;
    private final RecoveryTargetHandler recoveryTarget;
    private final int maxConcurrentFileChunks;
    private final int maxConcurrentOperations;
    private final ThreadPool threadPool;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();
    private final ListenableFuture<RecoveryResponse> future = new ListenableFuture<>();

    public SegmentReplicationSourceHandler(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        ThreadPool threadPool,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations
    ) {
        this.shard = shard;
        this.recoveryTarget = recoveryTarget;
        this.threadPool = threadPool;
        this.request = request;
        this.shardId = this.request.shardId().id();
        this.logger = Loggers.getLogger(getClass(), request.shardId(), "recover to " + request.targetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        // if the target is on an old version, it won't be able to handle out-of-order file chunks.
        this.maxConcurrentFileChunks = request.targetNode().getVersion().onOrAfter(LegacyESVersion.V_6_7_0) ? maxConcurrentFileChunks : 1;
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    public StartRecoveryRequest getRequest() {
        return request;
    }

    public void addListener(ActionListener<RecoveryResponse> listener) {
        future.addListener(listener, OpenSearchExecutors.newDirectExecutorService());
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public void recoverToTarget(ActionListener<RecoveryResponse> listener) {
        logger.info("SeqNoStats on Primary: {}", shard.seqNoStats());
        addListener(listener);
        final Closeable releaseResources = () -> IOUtils.close(resources);
        try {
            cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
                final RuntimeException e;
                if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                    e = new IndexShardClosedException(shard.shardId(), "shard is closed and recovery was canceled reason [" + reason + "]");
                } else {
                    e = new CancellableThreads.ExecutionCancelledException("recovery was canceled reason [" + reason + "]");
                }
                if (beforeCancelEx != null) {
                    e.addSuppressed(beforeCancelEx);
                }
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
                throw e;
            });
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(SegmentReplicationSourceHandler.this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
            };

            runUnderPrimaryPermit(() -> {
                    final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                    ShardRouting targetShardRouting = routingTable.getByAllocationId(request.targetAllocationId());
                    if (targetShardRouting == null) {
                        logger.debug(
                            "delaying recovery of {} as it is not listed as assigned to target node {}",
                            request.shardId(),
                            request.targetNode()
                        );
                        throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
                    }
                },
                shardId + " validating recovery target [" + request.targetAllocationId() + "] registered ",
                shard,
                cancellableThreads,
                logger
            );

            final StepListener<SendFileResult> sendFileStep = new StepListener<>();
            final StepListener<Void> sendInfosBytesStep = new StepListener<>();

            Engine.SegmentInfosRef segmentInfosRef = shard.getSegmentInfos();
            try {
                resources.add(segmentInfosRef);
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "snapshot failed", e);
            }

            try {

                final Releasable releaseStore = acquireStore(shard.store());
                resources.add(releaseStore);
                sendInfosBytesStep.whenComplete(r -> IOUtils.close(segmentInfosRef, releaseStore), e -> {
                    try {
                        IOUtils.close(segmentInfosRef, releaseStore);
                    } catch (final IOException ex) {
                        logger.warn("releasing snapshot caused exception", ex);
                    }
                });

                sendFileStep.whenComplete(r -> {
                    sendInfosBytes(segmentInfosRef.getInfos(), sendInfosBytesStep);
                }, listener::onFailure);

                phase1(segmentInfosRef.getInfos(), sendFileStep);

            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
            }

            sendInfosBytesStep.whenComplete(r -> {
                final long phase1ThrottlingWaitTime = 0L; // TODO: return the actual throttle time
                final SendFileResult sendFileResult = sendFileStep.result();
                final RecoveryResponse response = new RecoveryResponse(
                    sendFileResult.phase1FileNames,
                    sendFileResult.phase1FileSizes,
                    sendFileResult.phase1ExistingFileNames,
                    sendFileResult.phase1ExistingFileSizes,
                    sendFileResult.totalSize,
                    sendFileResult.existingTotalSize,
                    sendFileResult.took.millis(),
                    phase1ThrottlingWaitTime,
                    sendFileResult.took.millis(),
                    UNKNOWN,
                    0L
                );
                try {
                    future.onResponse(response);
                } finally {
                    IOUtils.close(resources);
                }
            }, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        }
    }

    static void runUnderPrimaryPermit(
        CancellableThreads.Interruptible runnable,
        String reason,
        IndexShard primary,
        CancellableThreads cancellableThreads,
        Logger logger
    ) {
        cancellableThreads.execute(() -> {
            CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }
            };
            primary.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, reason);
            try (Releasable ignored = FutureUtils.get(permit)) {
                // check that the IndexShard still has the primary authority. This needs to be checked under operation permit to prevent
                // races, as IndexShard will switch its authority only when it holds all operation permits, see IndexShard.relocated()
                if (primary.isRelocatedPrimary()) {
                    throw new IndexShardRelocatedException(primary.shardId());
                }
                runnable.run();
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }
        });
    }

    /**
     * Increases the store reference and returns a {@link Releasable} that will decrease the store reference using the generic thread pool.
     * We must never release the store using an interruptible thread as we can risk invalidating the node lock.
     */
    private Releasable acquireStore(Store store) {
        store.incRef();
        return Releasables.releaseOnce(() -> runWithGenericThreadPool(store::decRef));
    }

    private void runWithGenericThreadPool(CheckedRunnable<Exception> task) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        assert threadPool.generic().isShutdown() == false;
        // TODO: We shouldn't use the generic thread pool here as we already execute this from the generic pool.
        // While practically unlikely at a min pool size of 128 we could technically block the whole pool by waiting on futures
        // below and thus make it impossible for the store release to execute which in turn would block the futures forever
        threadPool.generic().execute(ActionRunnable.run(future, task));
        FutureUtils.get(future);
    }

    static final class SendFileResult {
        final List<String> phase1FileNames;
        final List<Long> phase1FileSizes;
        final long totalSize;

        final List<String> phase1ExistingFileNames;
        final List<Long> phase1ExistingFileSizes;
        final long existingTotalSize;

        final TimeValue took;

        SendFileResult(
            List<String> phase1FileNames,
            List<Long> phase1FileSizes,
            long totalSize,
            List<String> phase1ExistingFileNames,
            List<Long> phase1ExistingFileSizes,
            long existingTotalSize,
            TimeValue took
        ) {
            this.phase1FileNames = phase1FileNames;
            this.phase1FileSizes = phase1FileSizes;
            this.totalSize = totalSize;
            this.phase1ExistingFileNames = phase1ExistingFileNames;
            this.phase1ExistingFileSizes = phase1ExistingFileSizes;
            this.existingTotalSize = existingTotalSize;
            this.took = took;
        }
    }

    /**
     * Perform phase1 of the recovery operations. Once this {@link IndexCommit}
     * indexCommit has been performed no commit operations (files being fsync'd)
     * are effectively allowed on this index until all recovery phases are done
     * <p>
     * Phase1 examines the segment files on the target node and copies over the
     * segments that are missing. Only segments that have the same size and
     * checksum can be reused
     */
    void phase1(SegmentInfos segmentInfos, ActionListener<SendFileResult> listener) {
        cancellableThreads.checkForCancel();
        final Store store = shard.store();
        try {
            StopWatch stopWatch = new StopWatch().start();
            final Store.MetadataSnapshot recoverySourceMetadata;
            try {
                recoverySourceMetadata = store.getMetadata(segmentInfos);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                shard.failShard("recovery", ex);
                throw ex;
            }
            if (canSkipPhase1(recoverySourceMetadata, request.metadataSnapshot()) == false) {
                final List<String> phase1FileNames = new ArrayList<>();
                final List<Long> phase1FileSizes = new ArrayList<>();
                final List<String> phase1ExistingFileNames = new ArrayList<>();
                final List<Long> phase1ExistingFileSizes = new ArrayList<>();

                // Total size of segment files that are recovered
                long totalSizeInBytes = 0;
                // Total size of segment files that were able to be re-used
                long existingTotalSizeInBytes = 0;

                // Generate a "diff" of all the identical, different, and missing
                // segment files on the target node, using the existing files on
                // the source node
                final Store.RecoveryDiff diff = recoverySourceMetadata.recoveryDiff(request.metadataSnapshot());
                for (StoreFileMetadata md : diff.identical) {
                    phase1ExistingFileNames.add(md.name());
                    phase1ExistingFileSizes.add(md.length());
                    existingTotalSizeInBytes += md.length();
                    if (logger.isTraceEnabled()) {
                        logger.info(
                            "recovery [phase1]: not recovering [{}], exist in local store and has checksum [{}]," + " size [{}]",
                            md.name(),
                            md.checksum(),
                            md.length()
                        );
                    }
                    totalSizeInBytes += md.length();
                }
                List<StoreFileMetadata> phase1Files = new ArrayList<>(diff.different.size() + diff.missing.size());
                phase1Files.addAll(diff.different);
                phase1Files.addAll(diff.missing);
                for (StoreFileMetadata md : phase1Files) {
                    if (request.metadataSnapshot().asMap().containsKey(md.name())) {
                        logger.trace(
                            "recovery [phase1]: recovering [{}], exists in local store, but is different: remote [{}], local [{}]",
                            md.name(),
                            request.metadataSnapshot().asMap().get(md.name()),
                            md
                        );
                    } else {
                        logger.trace("recovery [phase1]: recovering [{}], does not exist in remote", md.name());
                    }
                    phase1FileNames.add(md.name());
                    phase1FileSizes.add(md.length());
                    totalSizeInBytes += md.length();
                }

                logger.info(
                    "recovery [phase1]: recovering_files [{}] with total_size [{}], reusing_files [{}] with total_size [{}]",
                    phase1FileNames.size(),
                    new ByteSizeValue(totalSizeInBytes),
                    phase1ExistingFileNames.size(),
                    new ByteSizeValue(existingTotalSizeInBytes)
                );
                final StepListener<Void> sendFileInfoStep = new StepListener<>();
                final StepListener<Void> sendFilesStep = new StepListener<>();
                final StepListener<Void> cleanFilesStep = new StepListener<>();
                cancellableThreads.checkForCancel();
                recoveryTarget.receiveFileInfo(
                    phase1FileNames,
                    phase1FileSizes,
                    phase1ExistingFileNames,
                    phase1ExistingFileSizes,
                    UNKNOWN,
                    sendFileInfoStep
                );

                sendFileInfoStep.whenComplete(
                    r -> sendFiles(store, phase1Files.toArray(new StoreFileMetadata[0]), sendFilesStep),
                    listener::onFailure
                );

                sendFilesStep.whenComplete(r -> {
                    final long lastKnownGlobalCheckpoint = shard.getLastKnownGlobalCheckpoint();
                    cleanFiles(store, recoverySourceMetadata, lastKnownGlobalCheckpoint, cleanFilesStep);
                }, listener::onFailure);

                final long totalSize = totalSizeInBytes;
                final long existingTotalSize = existingTotalSizeInBytes;
                cleanFilesStep.whenComplete(r -> {
                    final TimeValue took = stopWatch.totalTime();
                    logger.info("Replication took [{}]", took);
                    listener.onResponse(
                        new SendFileResult(
                            phase1FileNames,
                            phase1FileSizes,
                            totalSize,
                            phase1ExistingFileNames,
                            phase1ExistingFileSizes,
                            existingTotalSize,
                            took
                        )
                    );
                }, listener::onFailure);
            } else {
                logger.info("skipping [phase1] since source and target have identical sync id [{}]", recoverySourceMetadata.getSyncId());
            }
        } catch (Exception e) {
            throw new RecoverFilesRecoveryException(request.shardId(), 0, new ByteSizeValue(0L), e);
        }
    }

    boolean canSkipPhase1(Store.MetadataSnapshot source, Store.MetadataSnapshot target) {
        if (source.getSyncId() == null || source.getSyncId().equals(target.getSyncId()) == false) {
            return false;
        }
        if (source.getNumDocs() != target.getNumDocs()) {
            throw new IllegalStateException(
                "try to recover "
                    + request.shardId()
                    + " from primary shard with sync id but number "
                    + "of docs differ: "
                    + source.getNumDocs()
                    + " ("
                    + request.sourceNode().getName()
                    + ", primary) vs "
                    + target.getNumDocs()
                    + "("
                    + request.targetNode().getName()
                    + ")"
            );
        }
        SequenceNumbers.CommitInfo sourceSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(source.getCommitUserData().entrySet());
        SequenceNumbers.CommitInfo targetSeqNos = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(target.getCommitUserData().entrySet());
        if (sourceSeqNos.localCheckpoint != targetSeqNos.localCheckpoint || targetSeqNos.maxSeqNo != sourceSeqNos.maxSeqNo) {
            final String message = "try to recover "
                + request.shardId()
                + " with sync id but "
                + "seq_no stats are mismatched: ["
                + source.getCommitUserData()
                + "] vs ["
                + target.getCommitUserData()
                + "]";
            assert false : message;
            throw new IllegalStateException(message);
        }
        return true;
    }

    private void sendInfosBytes(SegmentInfos infos, ActionListener<Void> listener) {
        try {
            ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
            try (ByteBuffersIndexOutput tmpIndexOutput =
                     new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
                infos.write(tmpIndexOutput);
            }
            byte[] infosBytes = buffer.toArrayCopy();
            recoveryTarget.receiveSegmentInfosBytes(infos.version, infos.getGeneration(), infosBytes, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static class OperationChunkRequest implements MultiChunkTransfer.ChunkRequest {
        final List<Translog.Operation> operations;
        final boolean lastChunk;

        OperationChunkRequest(List<Translog.Operation> operations, boolean lastChunk) {
            this.operations = operations;
            this.lastChunk = lastChunk;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }
    }

    private class OperationBatchSender extends MultiChunkTransfer<Translog.Snapshot, OperationChunkRequest> {
        private final long startingSeqNo;
        private final long endingSeqNo;
        private final Translog.Snapshot snapshot;
        private final long maxSeenAutoIdTimestamp;
        private final long maxSeqNoOfUpdatesOrDeletes;
        private final RetentionLeases retentionLeases;
        private final long mappingVersion;
        private int lastBatchCount = 0; // used to estimate the count of the subsequent batch.
        private final AtomicInteger skippedOps = new AtomicInteger();
        private final AtomicInteger sentOps = new AtomicInteger();
        private final AtomicLong targetLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        OperationBatchSender(
            long startingSeqNo,
            long endingSeqNo,
            Translog.Snapshot snapshot,
            long maxSeenAutoIdTimestamp,
            long maxSeqNoOfUpdatesOrDeletes,
            RetentionLeases retentionLeases,
            long mappingVersion,
            ActionListener<Void> listener
        ) {
            super(logger, threadPool.getThreadContext(), listener, maxConcurrentOperations, Collections.singletonList(snapshot));
            this.startingSeqNo = startingSeqNo;
            this.endingSeqNo = endingSeqNo;
            this.snapshot = snapshot;
            this.maxSeenAutoIdTimestamp = maxSeenAutoIdTimestamp;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
            this.retentionLeases = retentionLeases;
            this.mappingVersion = mappingVersion;
        }

        @Override
        protected synchronized OperationChunkRequest nextChunkRequest(Translog.Snapshot snapshot) throws IOException {
            // We need to synchronized Snapshot#next() because it's called by different threads through sendBatch.
            // Even though those calls are not concurrent, Snapshot#next() uses non-synchronized state and is not multi-thread-compatible.
            assert Transports.assertNotTransportThread("[phase2]");
            cancellableThreads.checkForCancel();
            final List<Translog.Operation> ops = lastBatchCount > 0 ? new ArrayList<>(lastBatchCount) : new ArrayList<>();
            long batchSizeInBytes = 0L;
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (shard.state() == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(request.shardId());
                }
                final long seqNo = operation.seqNo();
                if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                    skippedOps.incrementAndGet();
                    continue;
                }
                ops.add(operation);
                batchSizeInBytes += operation.estimateSize();
                sentOps.incrementAndGet();

                // check if this request is past bytes threshold, and if so, send it off
                if (batchSizeInBytes >= chunkSizeInBytes) {
                    break;
                }
            }
            lastBatchCount = ops.size();
            return new OperationChunkRequest(ops, operation == null);
        }

        @Override
        protected void executeChunkRequest(OperationChunkRequest request, ActionListener<Void> listener) {
            cancellableThreads.checkForCancel();
            recoveryTarget.indexTranslogOperations(
                request.operations,
                snapshot.totalOperations(),
                maxSeenAutoIdTimestamp,
                maxSeqNoOfUpdatesOrDeletes,
                retentionLeases,
                mappingVersion,
                ActionListener.delegateFailure(listener, (l, newCheckpoint) -> {
                    targetLocalCheckpoint.updateAndGet(curr -> SequenceNumbers.max(curr, newCheckpoint));
                    l.onResponse(null);
                })
            );
        }

        @Override
        protected void handleError(Translog.Snapshot snapshot, Exception e) {
            throw new RecoveryEngineException(shard.shardId(), 2, "failed to send/replay operations", e);
        }

        @Override
        public void close() throws IOException {
            snapshot.close();
        }
    }

    static final class SendSnapshotResult {
        final long targetLocalCheckpoint;
        final int sentOperations;
        final TimeValue tookTime;

        SendSnapshotResult(final long targetLocalCheckpoint, final int sentOperations, final TimeValue tookTime) {
            this.targetLocalCheckpoint = targetLocalCheckpoint;
            this.sentOperations = sentOperations;
            this.tookTime = tookTime;
        }
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
        recoveryTarget.cancel();
    }

    @Override
    public String toString() {
        return "ShardRecoveryHandler{"
            + "shardId="
            + request.shardId()
            + ", sourceNode="
            + request.sourceNode()
            + ", targetNode="
            + request.targetNode()
            + '}';
    }

    private static class FileChunk implements MultiChunkTransfer.ChunkRequest, Releasable {
        final StoreFileMetadata md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        final Releasable onClose;

        FileChunk(StoreFileMetadata md, BytesReference content, long position, boolean lastChunk, Releasable onClose) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
            this.onClose = onClose;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    void sendFiles(Store store, StoreFileMetadata[] files, ActionListener<Void> listener) {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length)); // send smallest first

        final MultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileSender = new MultiChunkTransfer<StoreFileMetadata, FileChunk>(
            logger,
            threadPool.getThreadContext(),
            listener,
            maxConcurrentFileChunks,
            Arrays.asList(files)
        ) {

            final Deque<byte[]> buffers = new ConcurrentLinkedDeque<>();
            InputStreamIndexInput currentInput = null;
            long offset = 0;

            @Override
            protected void onNewResource(StoreFileMetadata md) throws IOException {
                offset = 0;
                IOUtils.close(currentInput, () -> currentInput = null);
                final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
                currentInput = new InputStreamIndexInput(indexInput, md.length()) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(indexInput, super::close); // InputStreamIndexInput's close is a noop
                    }
                };
            }

            private byte[] acquireBuffer() {
                final byte[] buffer = buffers.pollFirst();
                if (buffer != null) {
                    return buffer;
                }
                return new byte[chunkSizeInBytes];
            }

            @Override
            protected FileChunk nextChunkRequest(StoreFileMetadata md) throws IOException {
                assert Transports.assertNotTransportThread("read file chunk");
                cancellableThreads.checkForCancel();
                final byte[] buffer = acquireBuffer();
                final int bytesRead = currentInput.read(buffer);
                if (bytesRead == -1) {
                    throw new CorruptIndexException("file truncated; length=" + md.length() + " offset=" + offset, md.name());
                }
                final boolean lastChunk = offset + bytesRead == md.length();
                final FileChunk chunk = new FileChunk(
                    md,
                    new BytesArray(buffer, 0, bytesRead),
                    offset,
                    lastChunk,
                    () -> buffers.addFirst(buffer)
                );
                offset += bytesRead;
                return chunk;
            }

            @Override
            protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                cancellableThreads.checkForCancel();
                recoveryTarget.writeFileChunk(
                    request.md,
                    request.position,
                    request.content,
                    request.lastChunk,
                    UNKNOWN,
                    ActionListener.runBefore(listener, request::close)
                );
            }

            @Override
            protected void handleError(StoreFileMetadata md, Exception e) throws Exception {
                handleErrorOnSendFiles(store, e, new StoreFileMetadata[]{md});
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(currentInput, () -> currentInput = null);
            }
        };
        resources.add(multiFileSender);
        multiFileSender.start();
    }

    private void cleanFiles(
        Store store,
        Store.MetadataSnapshot sourceMetadata,
        long globalCheckpoint,
        ActionListener<Void> listener
    ) {
        // Send the CLEAN_FILES request, which takes all of the files that
        // were transferred and renames them from their temporary file
        // names to the actual file names. It also writes checksums for
        // the files after they have been renamed.
        //
        // Once the files have been renamed, any other files that are not
        // related to this recovery (out of date segments, for example)
        // are deleted
        cancellableThreads.checkForCancel();
        recoveryTarget.cleanFiles(
            UNKNOWN,
            globalCheckpoint,
            sourceMetadata,
            ActionListener.delegateResponse(listener, (l, e) -> ActionListener.completeWith(l, () -> {
                StoreFileMetadata[] mds = StreamSupport.stream(sourceMetadata.spliterator(), false).toArray(StoreFileMetadata[]::new);
                ArrayUtil.timSort(mds, Comparator.comparingLong(StoreFileMetadata::length)); // check small files first
                handleErrorOnSendFiles(store, e, mds);
                throw e;
            }))
        );
    }

    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(SegmentReplicationSourceHandler.this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetadata md : mds) {
                cancellableThreads.checkForCancel();
                logger.debug("checking integrity for file {} after remove corruption exception", md);
                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                    if (localException == null) {
                        localException = corruptIndexException;
                    }
                    failEngine(corruptIndexException);
                }
            }
            if (localException != null) {
                throw localException;
            } else { // corruption has happened on the way to replica
                RemoteTransportException remoteException = new RemoteTransportException(
                    "File corruption occurred on recovery but checksums are ok",
                    null
                );
                remoteException.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "{} Remote file corruption on node {}, recovering {}. local checksum OK",
                        shardId,
                        request.targetNode(),
                        mds
                    ),
                    corruptIndexException
                );
                throw remoteException;
            }
        }
        throw e;
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
