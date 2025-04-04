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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.AsyncIOProcessor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ReplicationSink.OperationDetails;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

/**
 * Implementation of IndexingOperationListener that collects operations
 * and organizes them into batches for uploading.
 */
@PublicApi(since = "3.0.0")
public class ReplicationOperationListener implements IndexingOperationListener {
    private final Queue<OperationDetails> operationsQueue;
    private final List<ReplicationSink> sinks;
    private final IndexShard shard;

    private final LocalCheckpointTracker tracker;

    private static final Logger logger = LogManager.getLogger(ReplicationOperationListener.class);
    private final AsyncIOProcessor<Long> processor;

    /**
     * ReplicationOperationListener - IndexOperationListener implementation that batches operations post ingestion and hands off to a {@link ReplicationSink}.
     *
     * @param sinks         - {@link List<ReplicationSink>}
     * @param threadContext - {@link ThreadContext}
     */
    public ReplicationOperationListener(IndexShard shard, List<ReplicationSink> sinks, ThreadContext threadContext) {
        this.shard = shard;
        this.operationsQueue = new ConcurrentLinkedQueue<>();
        this.sinks = sinks;
        // we need to track which checkpoints have been uploaded in sequence given postIndex/Delete calls can be invoked out of order
        // creating our own tracker here, the alternative is we reuse the shards tracker with new checkpoints...
        // init this to NO_OPS_PERFORMED we will fast forward the cps after the engine is opened to the shards processed cp
        // processed = staged for upload
        // persisted = uploaded
        this.tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        this.processor = new AsyncIOProcessor<>(logger, 12345, threadContext) {
            @Override
            protected void write(List<Tuple<Long, Consumer<Exception>>> candidates) throws IOException {
                assert candidates.isEmpty() == false;
                // find the max of the listeners
                long maxSeqNo = candidates.stream().mapToLong(Tuple::v1).max().getAsLong();
                if (tracker.getPersistedCheckpoint() >= maxSeqNo) {
                    return;
                }
                List<OperationDetails> operationDetails = pollUntil(maxSeqNo);
                assert operationDetails.isEmpty() == false;
                // we async invoke each sink, but only wait for the synchronous ones to complete before completion.
                final CountDownLatch latch = new CountDownLatch(sinks.size());
                List<ReplicationSinkException> exceptionList = new ArrayList<>();
                LatchedActionListener<Long> latchedActionListener = new LatchedActionListener<>(
                    ActionListener.wrap(l -> {}, ex -> {
                        assert ex instanceof ReplicationSinkException;
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Exception during transfer for file {}",
                                ex
                            ),
                            ex
                        );
                        exceptionList.add((ReplicationSinkException) ex);
                    }),
                    latch
                );

                // we should spawn threads here for this so a bad impl can't block
                for (ReplicationSink sink : sinks) {
                    sink.acceptBatch(shard.shardId, operationDetails, latchedActionListener);
                }
                try {
                    if (latch.await(30, TimeUnit.SECONDS) == false) {
                        ReplicationSinkException ex = new ReplicationSinkException(
                            "Timed out waiting for replication sinks to complete "
                        );
                        exceptionList.forEach(ex::addSuppressed);
                        throw ex;
                    }
                } catch (InterruptedException ex) {
                    // latch failed to complete before timeout, mark all listeners as no ops performed.
                    ReplicationSinkException exception = new ReplicationSinkException("Failed", ex, SequenceNumbers.NO_OPS_PERFORMED);
                    exceptionList.forEach(exception::addSuppressed);
                    Thread.currentThread().interrupt();
                    throw exception;
                }
                if (exceptionList.isEmpty() == false) {
                    // maybe honor a partial failure...
                    // get the minimum seqNo that all sinks completed or NO_OP.
                    // this may be higher than some of the listeners that were registered in the batch.
                    // A single Exception will be thrown to all batches its on the caller to determine if partial result should be returned.
                    OptionalLong minCompleted = exceptionList.stream().mapToLong(ReplicationSinkException::getMaxReplicated).min();
                    minCompleted.ifPresent(tracker::fastForwardPersistedSeqNo);
                    throw new ReplicationSinkException("Failed", minCompleted.orElse(NO_OPS_PERFORMED));
                }
                // fast fwd the persisted to the processed
                tracker.fastForwardPersistedSeqNo(tracker.getProcessedCheckpoint());
                // we may have uploaded higher seqNos, so mark those as well
                for (OperationDetails operationDetail : operationDetails) {
                    tracker.markSeqNoAsPersisted(operationDetail.seqNo());
                }
                assert maxSeqNo <= tracker.getPersistedCheckpoint() : "failed " + maxSeqNo + " " + tracker.getPersistedCheckpoint();
            }
        };
    }

    void initializeSeqNoTracker(long seqNo) {
        tracker.fastForwardProcessedSeqNo(seqNo);
        tracker.fastForwardPersistedSeqNo(seqNo);
    }

    long getSeenCheckpoint() {
        return tracker.getProcessedCheckpoint();
    }

    long getReplicatedCheckpoint() {
        return tracker.getPersistedCheckpoint();
    }

    // does translog source contain whole doc or only delta - A: whole
    // does parsedDoc contain the delta or whole doc - A: whole
    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.isCreated() || result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new ReplicationSink.IndexingOperationDetails(
                index.id(),
                index.uid(),
                result.getSeqNo(),
                result.getTerm(),
                index.parsedDoc()
            );
            operationsQueue.add(details);
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new ReplicationSink.DeleteOperationDetails(
                delete.id(),
                delete.uid(),
                result.getSeqNo(),
                result.getTerm()
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

        while (tracker.getProcessedCheckpoint() < requireProcessed) {
            // we could have a requireProcessed seqNo that is higher than anything that has hit our queue
            // this happens when we have a concurrent write that hasn't hit the listener yet, but the higher seqNo has.
            // in this case, continue until we poll the required op.
            op = operationsQueue.poll();
            if (op == null) continue;
            tracker.markSeqNoAsProcessed(op.seqNo());
            // Only add the operation to the map if it has a higher seqNo than what's currently there
            // or if there's no operation yet for this document ID
            OperationDetails finalOp = op;
            docIdToOperations.compute(op.docId(), (docId, existingOp) -> {
                if (existingOp == null) return finalOp;
                if (existingOp instanceof ReplicationSink.IndexingOperationDetails && finalOp instanceof ReplicationSink.IndexingOperationDetails) {
                    ReplicationSink.IndexingOperationDetails existing = (ReplicationSink.IndexingOperationDetails) existingOp;
                    ReplicationSink.IndexingOperationDetails fo = (ReplicationSink.IndexingOperationDetails) finalOp;
                    // in this case we have two index requests.  We need to merge the source & parsed documents recursively
                    // so that updates are reduced to a single view of the document.
                    return merge(existing, fo);
                } else {
                    return finalOp.seqNo() > existingOp.seqNo() ? finalOp : existingOp;
                }
            });
        }
        assert tracker.getProcessedCheckpoint() >= requireProcessed;
        return new ArrayList<>(docIdToOperations.values());
    }

    private OperationDetails merge(ReplicationSink.IndexingOperationDetails iod, ReplicationSink.IndexingOperationDetails fo) {
        ReplicationSink.IndexingOperationDetails high = iod.seqNo() > fo.seqNo() ? iod : fo;
        ReplicationSink.IndexingOperationDetails low = high == iod ? fo : iod;

        final Map<String, Object> lowMap = XContentHelper.convertToMap(low.parsedDoc().source(), true, low.parsedDoc().getMediaType()).v2();
        final Map<String, Object> highMap = XContentHelper.convertToMap(high.parsedDoc().source(), true, high.parsedDoc().getMediaType()).v2();
        boolean update = XContentHelper.update(lowMap, highMap, false);
        if (update == false) {
            // no changes just take the final op.
            return fo;
        }
        try {
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(low.parsedDoc().getMediaType());
            builder.map(lowMap);

            SourceToParse source = new SourceToParse(
                shard.shardId.getIndexName(),
                low.parsedDoc().id(),
                BytesReference.bytes(builder),
                MediaTypeRegistry.xContentType(low.parsedDoc().source()),
                low.parsedDoc().routing()
            );
            ParsedDocument updatedDoc = shard.mapperService().documentMapper().parse(source);
            return new ReplicationSink.IndexingOperationDetails(low.docId(), low.uId(), high.seqNo(), high.primaryTerm(), updatedDoc);
        } catch (Exception e) {
            throw new ReplicationSinkException("Unable to reduce operations for single doc within set", e, NO_OPS_PERFORMED);
        }
    }

    // wait until a seqNo has been uploaded, or notified of a problem
    public void addListener(Long seqNo, Consumer<Exception> callback) {
        processor.put(seqNo, callback);
    }
}
