/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.AsyncIOProcessor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

/**
 * Implementation of IndexingOperationListener that collects operations
 * and organizes them into batches for uploading.
 */
@PublicApi(since = "3.0.0")
public class BatchingIndexingOperationListener implements IndexingOperationListener {
    // hold two queues as ops come in out of order to the listener
    private final Queue<OperationDetails> operationsQueue;
    private final Queue<OperationDetails> orderedQueue;

    private LocalCheckpointTracker tracker;

    private static final Logger logger = LogManager.getLogger(BatchingIndexingOperationListener.class);
    private final AsyncIOProcessor<Long> processor;

    public BatchingIndexingOperationListener(ThreadContext threadContext) {
        // default init capacity
        this.operationsQueue = new PriorityBlockingQueue<>(11, Comparator.comparingLong(OperationDetails::seqNo));
        this.orderedQueue = new PriorityQueue<>();
        // we need to track which checkpoints have been uploaded in sequence given postIndex/Delete calls can be invoked out of order
        // creating our own tracker here, the alternative is we reuse the shards tracker with new checkpoints...
        // init this to NO_OPS_PERFORMED we will fast forward the cps after the engine is opened to the shards processed cp
        // processed = staged for upload
        // persisted = uploaded
        this.tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        this.processor = new AsyncIOProcessor<>(logger, 12345, threadContext) {
            @Override
            protected void write(List<Tuple<Long, Consumer<Exception>>> candidates) throws IOException {
                // find the max of the listeners
                Optional<Tuple<Long, Consumer<Exception>>> max = candidates.stream().max(Comparator.comparingLong(Tuple::v1));
                Long maxSeqNo = max.get().v1();
                List<OperationDetails> operationDetails = pollUntil(maxSeqNo);
                chunkAndUpload(operationDetails);
                tracker.fastForwardPersistedSeqNo(maxSeqNo);
            }
        };
    }

    private void chunkAndUpload(List<OperationDetails> operationDetails) {
        // do the thing
    }

    void initializeSeqNoTracker(long seqNo) {
        tracker.fastForwardProcessedSeqNo(seqNo);
        tracker.fastForwardPersistedSeqNo(seqNo);
    }

    long getSeenCheckpoint() {
        return tracker.getProcessedCheckpoint();
    }

    long getUploadedCheckpoint() {
        return tracker.getPersistedCheckpoint();
    }

    // does translog source contain whole doc or only delta - A: whole
    // does parsedDoc contain the delta or whole doc - A: whole
    @Override
    public void postIndex(ShardId indexShard, Engine.Index index, Engine.IndexResult result) {
        if (result.isCreated() || result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new OperationDetails(
                index.id(),
                index.uid(),
                result.getSeqNo(), // Use sequence number from result
                result.getTerm(),  // Use primary term from result
                false,             // Not a delete
                index.parsedDoc()  // Store the parsed document
            );
            operationsQueue.add(details);
            logger.info(SourceLookup.sourceAsMap(index.source()));
            logger.info(SourceLookup.sourceAsMap(index.parsedDoc().source()));
        }
    }

    @Override
    public void postDelete(ShardId indexShard, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new OperationDetails(
                delete.id(),
                delete.uid(),
                result.getSeqNo(), // Use sequence number from result
                result.getTerm(),  // Use primary term from result
                true,              // Is a delete
                null               // No parsed document for delete operations
            );
            operationsQueue.add(details);
        }
    }

    /**
     * Polls operations from the queue until a given seqNo
     *
     * @param requireProcessed max seqNo that must be included in the batch
     * @return List of batches, each containing OperationDetails objects
     */
    List<OperationDetails> pollUntil(Long requireProcessed) {
        OperationDetails op;
        // maintain a map of the doc id to operation to be uploaded, until we process all required seqNos.
        Map<String, OperationDetails> docIdToOperations = new HashMap<>();
        while (tracker.getProcessedCheckpoint() < requireProcessed && (op = operationsQueue.poll()) != null) {
            // Step 1: Poll operations from the queue
            tracker.markSeqNoAsProcessed(op.seqNo);
            // Only add the operation to the map if it has a higher seqNo than what's currently there
            // or if there's no operation yet for this document ID
            OperationDetails finalOp = op;
            docIdToOperations.compute(op.docId, (docId, existingOp) ->
                existingOp == null || finalOp.seqNo > existingOp.seqNo ? finalOp : existingOp
            );
        }
        return docIdToOperations.values().stream().toList();
    }

    // wait until a seqNo has been uploaded, or notified of a problem
    public void addListener(Long seqNo, Consumer<Exception> callback) {
        processor.put(seqNo, callback);
    }

    /**
     * Custom class to store operation details with the correct sequence numbers from results
     *
     * @param parsedDoc null for delete operations
     */
    @PublicApi(since = "3.0.0")
    public record OperationDetails(String docId, Term uId, long seqNo, long primaryTerm, boolean isDelete,
                                   ParsedDocument parsedDoc) {

        @Override
        public String toString() {
            return "OperationDetails{" +
                "docId='" + docId + '\'' +
                ", seqNo=" + seqNo +
                ", isDelete=" + isDelete +
                '}';
        }
    }
}

// notes...

// An implementation of IndexingOperationListener in OpenSearch that does the following:
// Collects results through postIndex & postDelete and adds the operations to a queue.
// Each operation correlates with a doc id (uid) and sequence number (seqNo), and the parsed document (index.doc) if an index operation.

// We poll a required number of operations from the queue to create a set that will be partitioned into batches for upload.
// Within the set, we can only have one operation per docId, and that operation should be the highest seqNo associated with that docId from the polled ops.
// The batches will be uploaded in parallel and have the following properties:
//  1. Across all batches all doc ids must be unique
//  2. To reduce duplicate operations for a doc id, we will need the following logic
