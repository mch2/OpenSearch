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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.*;

import java.io.IOException;
import java.util.List;

/**
 * Represents a recovery where the current node is the target node of the recovery. To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 */
public class ReplicationTarget extends RecoveryTarget {

    private final Logger logger;

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard local shard where we want to recover to
     * @param sourceNode source node of the recovery where we recover from
     * @param listener   called when recovery is completed/failed
     */
    public ReplicationTarget(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener) {
        super(indexShard, sourceNode, listener);
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
    }

    @Override
    public void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            final RecoveryState.Index index = state().getIndex();
            for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
                logger.info("existing file: {}", phase1ExistingFileNames.get(i));
                index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
            }
            for (int i = 0; i < phase1FileNames.size(); i++) {
                logger.info("new file: {}", phase1FileNames.get(i));
                index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
            }
            index.setFileDetailsComplete();
            state().getTranslog().totalOperations(totalTranslogOps);
            state().getTranslog().totalOperationsOnStart(totalTranslogOps);
            return null;
        });
    }

    @Override
    public void cleanFiles(
        int totalTranslogOps,
        long globalCheckpoint,
        Store.MetadataSnapshot sourceMetadata,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            try {
                state().getTranslog().totalOperations(totalTranslogOps);
                // first, we go and move files that were created with the recovery id suffix to
                // the actual names, its ok if we have a corrupted index here, since we have replicas
                // to recover from in case of a full cluster shutdown just when this code executes...
                String translogUUID = indexShard.getTranslog().getTranslogUUID();
                logger.info("TranslogUUID {}", translogUUID);
                indexShard.closeWriter();
                logger.info("Start renaming files");
                multiFileWriter.renameAllTempFiles();
                logger.info("Completed renaming files");

//                indexShard.fillSeqNoGaps();
                logger.info("SeqNo updated...");
                final Store store = store();
                store.incRef();
                store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetadata);

                for (String s : store.directory().listAll()) {
                    logger.info("FILES IN REPLICATIONTARGET: {}", s);
                }

            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                logger.error("Error", ex);
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.error("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                logger.error("What broken", ex);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    @Override
    public void receiveSegmentInfosBytes(long version, long gen, byte[] infosBytes, ActionListener<Void> listener) {
        try {
            indexShard.updateCurrentInfos(version, gen, infosBytes);
            // open up a new writer
            try {
//                indexShard.openIW();
            } catch (Exception e) {
                logger.error("Error opening writer", e);
            }
            for (String s : store.directory().listAll()) {
                logger.info("FILES AFTER IW OPENED: {}", s);
            }
            listener.onResponse(null);
        } catch (IOException ex) {
            logger.error("Error", ex);
            listener.onFailure(ex);

        }
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            indexShard.updateGlobalCheckpointOnReplica(globalCheckpoint, "finalizing recovery");
            // Persist the global checkpoint.
            indexShard.sync();
            indexShard.persistRetentionLeases();
            if (trimAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // We should erase all translog operations above trimAboveSeqNo as we have received either the same or a newer copy
                // from the recovery source in phase2. Rolling a new translog generation is not strictly required here for we won't
                // trim the current generation. It's merely to satisfy the assumption that the current generation does not have any
                // operation that would be trimmed (see TranslogWriter#assertNoSeqAbove). This assumption does not hold for peer
                // recovery because we could have received operations above startingSeqNo from the previous primary terms.
                indexShard.rollTranslogGeneration();
                // the flush or translog generation threshold can be reached after we roll a new translog
                indexShard.afterWriteOperation();
                indexShard.trimOperationOfPreviousPrimaryTerms(trimAboveSeqNo);
            }
            if (hasUncommittedOperations()) {
                indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            }
//            indexShard.finalizeRecovery();
            return null;
        });
    }

    /**
     * mark the current recovery as done
     */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            listener.onRecoveryDone(state());
        }
    }
}
