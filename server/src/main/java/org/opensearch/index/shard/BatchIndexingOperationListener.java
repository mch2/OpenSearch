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
import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

/**
 * Implementation of IndexingOperationListener that collects operations
 * and organizes them into batches to send to a sink.
 * <p>
 * All incoming index/delete operations are added to a queue after processed by the index.
 * <p>
 * We register a listener per write request against a BufferedAsyncIOProcessor, that in its write method will poll from the queue, dedupe operations by Doc ID, and send batches to registered
 * {@link Sink}s.
 * <p>
 * Write requests register a callback to get alerted when all operations in the request have been processed by sinks exactly once.
 * If any operation fails to be processed by a sink, all requests in the batch will fail if they have a seqNo at or lower than the first failed operation in the batch.
 */
public class BatchIndexingOperationListener implements IndexingOperationListener {
    private final Queue<OperationDetails> operationsQueue;
    private final Set<Sink> sinks;
    private final ShardId shardId;

    // We use a LocalCheckpointTracker to track incoming operations.
    // processed means we have polled the operation from the queue and attempted to pass it to a sink ie. Seen the seqNo.
    // persisted means the operation either was handled by the sink successfully or part of a failed batch *and* was part of a request
    // passed to BufferedAsyncIOProcessor#Write method. ie. Completed. This allows us to track across batches if a seqNo
    // was uploaded from a prior batch, but the req waiting on the seqNo is passed to write in a future batch.
    private final LocalCheckpointTracker tracker;

    private static final Logger logger = LogManager.getLogger(BatchIndexingOperationListener.class);
    private final BufferedAsyncIOProcessor<SortedSet<Long>> processor;

    // the listener is wired up to any shard type as long as there are configured sinks on the index
    // noop this listener if the shard is not primary.
    private final AtomicBoolean active = new AtomicBoolean(false);

    /**
     * ReplicationOperationListener - IndexOperationListener implementation that batches operations post ingestion and hands off to a {@link Sink}.
     *
     * @param sinks      - {@link List< Sink >}
     * @param threadPool - {@link ThreadPool}
     */
    public BatchIndexingOperationListener(
        ShardId shardId,
        Set<Sink> sinks,
        ThreadPool threadPool,
        RemoteStoreSettings remoteStoreSettings
    ) {
        this.shardId = shardId;
        this.operationsQueue = new ConcurrentLinkedQueue<>();
        this.sinks = sinks;
        this.tracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
        this.processor = new BufferedAsyncIOProcessor<>(
            logger,
            102400,
            threadPool.getThreadContext(),
            threadPool,
            remoteStoreSettings::getClusterRemoteTranslogBufferInterval
        ) {

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.REPLICATION_SINKS;
            }

            @Override
            protected void write(List<Tuple<SortedSet<Long>, Consumer<Exception>>> requests) {
                assert requests.isEmpty() == false;
                assert active.get();
                assert tracker.getPersistedCheckpoint() <= tracker.getProcessedCheckpoint();

                // check if a prior batch has failed that impacts the incoming batch. Any incoming req containing a seqNo that has
                // previously failed should be marked as failed and included in this set.
                // If this set is non empty, this batch will return an Exception and include a set of seqNos uniquely identifying the failed
                // reqs (max seqNo of the req).
                // for any failed req included in this set, we will also mark all of its seqNos as persisted and move up our checkpoint
                // given we will not see a req with it again.
                final Set<Long> failed = tracker.getPersistedCheckpoint() < tracker.getProcessedCheckpoint()
                    ? getAlreadyErroredRequests(requests)
                    : new HashSet<>();

                failed.addAll(processNewBatch(requests));

                completeRequest(failed);
            }
        };
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        assert active.get();
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new IndexOperationDetails(index.id(), result.getSeqNo(), result.getTerm(), index.parsedDoc());
            operationsQueue.add(details);
            logger.trace("Queueing Index op for {} {}", details.seqNo(), details.docId());
        } else {
            handleDocumentFailure(result);
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        assert active.get();
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            OperationDetails details = new DeleteOperationDetails(delete.id(), result.getSeqNo(), result.getTerm());
            operationsQueue.add(details);
            logger.trace("Queueing Delete op for {} {}", details.seqNo(), details.docId());
        } else {
            handleDocumentFailure(result);
        }
    }

    /**
     * Register a Listener per write request with the set of SequenceNumbers indexed.
     * <p>
     * Listeners will only complete once all operations have been processed exactly once.
     * <p>
     * If any seqNo in the request fails to be processed an Exception is returned.
     *
     * @param seqNos   {@link SortedSet<Long>} Sorted set of sequence numbers.
     * @param listener {@link Consumer<Exception>} callback invoked when all operations have been processed.
     */
    public void addListener(SortedSet<Long> seqNos, Consumer<Exception> listener) {
        processor.put(seqNos, e -> listener.accept(didRequestSucceed(seqNos, e) ? null : e));
    }

    /**
     * Initialize the listener to a starting seqNo, his should be invoked
     * on engine open.
     *
     * @param seqNo - starting seqNo
     */
    void initializeReplicationOperationListener(long seqNo) {
        assert active.get() == false;
        assert operationsQueue.isEmpty();
        if (active.getAndSet(true) == false) {
            logger.trace("Initialing tracker with {}", seqNo);
            tracker.fastForwardProcessedSeqNo(seqNo);
            tracker.fastForwardPersistedSeqNo(seqNo);
        }
    }

    /**
     * Check if a request succeeded with partial failure.
     * The req will fail iff Exception is non null or the seqNo is not included in the set of failed requests.
     *
     * @param req - max seqNo in the request
     * @param e   - Error
     * @return
     */
    private boolean didRequestSucceed(SortedSet<Long> req, Exception e) {
        if (e == null) return true;
        if (e instanceof ReplicationSinkException) {
            ReplicationSinkException rse = (ReplicationSinkException) e;
            return rse.getFailed().contains(req.last()) == false;
        }
        // if failed for any other reason, mark the seqNos as persisted even if
        // they have not been polled from the queue.
        req.forEach(tracker::markSeqNoAsPersisted);
        return false;
    }

    long getSeenCheckpoint() {
        return tracker.getProcessedCheckpoint();
    }

    long getCompletedCheckpoint() {
        return tracker.getPersistedCheckpoint();
    }

    boolean hasProcessed(long seqNo) {
        return tracker.hasProcessed(seqNo);
    }

    boolean hasCompleted(long seqNo) {
        return tracker.hasPersisted(seqNo);
    }

    /**
     * From a single batch, figure out if any of the requests had seqNos that failed in a prior batch.
     * We do this by figuring out if there were any prior failures unaccounted for by checking the processed seqNo
     * against the current persisted seqNo.  If the processed seqno is higher than persisted, iterate through each incoming
     * req and check each seqNo's processed/persisted state.  If any seqno is processed but not yet persisted, it had failed in a prior batch.
     */
    private Set<Long> getAlreadyErroredRequests(List<Tuple<SortedSet<Long>, Consumer<Exception>>> reqs) {
        assert tracker.getPersistedCheckpoint() < tracker.getProcessedCheckpoint();
        return reqs.stream().filter(candidate -> {
            // if we haven't processed the first seqno, don't iterate over all seqnos.
            if (tracker.hasProcessed(candidate.v1().first()) == false) return false;

            boolean hasFailedOp = candidate.v1().stream().anyMatch(this::failedInPreviousBatch);

            if (hasFailedOp) {
                // Mark all sequence numbers as persisted if any failed, we don't want to pass those to the sink
                candidate.v1().forEach(tracker::markSeqNoAsPersisted);
            }
            return hasFailedOp;
        }).map(candidate -> candidate.v1().last()).collect(Collectors.toSet());
    }

    private Set<Long> processNewBatch(List<Tuple<SortedSet<Long>, Consumer<Exception>>> requests) {
        // Create one sorted batch with every seqNo, this still needs to be deduped by docId which will occur in processNewBatch.
        final SortedSet<Long> batch = requests.stream()
            .map(Tuple::v1)
            .collect(Collectors.toList())
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(TreeSet::new));

        // assert any op going forward in the batch has either never been seen or is already completed
        assert batch.stream().allMatch(seqNo -> tracker.hasProcessed(seqNo) == false || tracker.hasPersisted(seqNo))
            : "Expected All seqNos in the request to be new or already marked completed"
                + batch.stream()
                    .filter(seqNo -> tracker.hasProcessed(seqNo) == false || tracker.hasPersisted(seqNo))
                    .collect(Collectors.toSet());

        // already completed all seqNos in the batch, complete early.
        if (batch.last() <= tracker.getPersistedCheckpoint()) {
            return Collections.emptySet();
        }

        // poll the next batch from the queue
        Tuple<Collection<OperationDetails>, Set<Long>> result = pollUntil(batch.last());
        final SortedSet<OperationDetails> operationDetails = new TreeSet<>(result.v1());
        assert operationDetails.isEmpty() == false
            || batch.stream().allMatch(seqNo -> tracker.hasProcessed(seqNo) && tracker.hasPersisted(seqNo))
            : "new batch should either all be persisted or empty: " + batch;

        long completedUpTo = batch.last(); // assume everything succeeds unless told otherwise.
        for (Sink sink : sinks) {
            completedUpTo = Math.min(sink.acceptBatch(shardId, operationDetails), completedUpTo);
        }
        return handleResult(requests, completedUpTo, operationDetails, result, batch);
    }

    private Set<Long> handleResult(
        List<Tuple<SortedSet<Long>, Consumer<Exception>>> requests,
        final long completedUpTo,
        SortedSet<OperationDetails> operationDetails,
        Tuple<Collection<OperationDetails>, Set<Long>> result,
        SortedSet<Long> batch
    ) {
        // mark every op that was sent to the sink and successful as persisted (this intentionally does not include deduped away operations)
        operationDetails.stream().filter(op -> op.seqNo() <= completedUpTo).forEach(op -> tracker.markSeqNoAsPersisted(op.seqNo()));

        // mark any operation that is part of the request batch (set of incoming requests to the asyncIO write) as persisted.
        batch.forEach(tracker::markSeqNoAsPersisted);

        // if we failed to process all ops check for failed requests
        if (completedUpTo < batch.last()) {
            if (result.v2().isEmpty() == false) {
                // if we had to dedupe by docId across requests, we need to fail all the requests
                // this ensures the deduped away req is also negatively ack'd.
                return requests.stream().map(tuple -> tuple.v1().last()).collect(Collectors.toSet());
            }
            // filter out reqs that passed (highest seqNo in the req is lte than completedUpTo)
            return requests.stream().map(tuple -> tuple.v1().last()).filter(last -> last > completedUpTo).collect(Collectors.toSet());
        }
        // everything passed, mark the deduped away numbers as completed
        result.v2().forEach(tracker::markSeqNoAsPersisted);
        return Collections.emptySet();
    }

    /**
     * Polls operations from the queue until a given seqNo or the queue is empty.
     *
     * @param requireProcessed max seqNo that must be included in the batch
     * @return Sorted set of batches, each containing OperationDetails objects
     */
    Tuple<Collection<OperationDetails>, Set<Long>> pollUntil(Long requireProcessed) {
        // maintain a map of the doc id to operation to be uploaded, until we process all required seqNos.
        Map<String, OperationDetails> docIdToOperations = new HashMap<>();
        // we can assert the queue is either empty or we've already processed all ops off the queue.
        assert operationsQueue.isEmpty() == false || tracker.getProcessedCheckpoint() >= requireProcessed
            : "Queue should never run empty until we've polled all expected ops processed cp: "
                + tracker.getProcessedCheckpoint()
                + " required: "
                + requireProcessed;
        Set<Long> deduped = new HashSet<>();
        while (tracker.getProcessedCheckpoint() < requireProcessed) {
            final OperationDetails op = operationsQueue.poll();
            assert op != null;
            tracker.markSeqNoAsProcessed(op.seqNo());

            // If the operation removed from the queue has already been marked completed ignore it and do not send to sink.
            // this happens when the operation is part of a req split across batches that we know will already fail, before
            // The first step of write is to check for failed requests and mark all of their ops as persisted before polling from the queue.
            // Example: Queue = [ R1(1,2,3), R2(4,5,6), R1 (7,8,9)] and the previous call to write processed only R2,
            // and say 3-6 fails. later when write is called with R1, we know that R1 already has failed ops in the previous call to write,
            // so we mark 7,8,9 as persisted and result in not being sent to sink.
            if (tracker.hasPersisted(op.seqNo())) continue;
            // if we have to dedupe, keep track of which op was deduped away in case of failure.
            // in the event of failure we do *not* want to mark the deduped away ops as persisted
            // they will only be marked persisted later if the request batch included the seqno.
            docIdToOperations.compute(op.docId(), (docId, existing) -> {
                if (existing == null) {
                    return op;
                }
                if (op.seqNo() > existing.seqNo()) {
                    deduped.add(existing.seqNo());
                    return op;
                } else {
                    deduped.add(op.seqNo());
                    return existing;
                }
            });
        }
        assert tracker.getProcessedCheckpoint() >= requireProcessed : "Expected "
            + tracker.getProcessedCheckpoint()
            + " to be at least "
            + requireProcessed;
        return new Tuple<>(docIdToOperations.values(), deduped);
    }

    private void completeRequest(Set<Long> failedRequests) {
        // check if all ops in the batch completed properly.
        if (failedRequests.isEmpty() == false) {
            // honor a partial failure allowing a subset of listeners in requests to complete if possible.
            // in this case we wrap the min in an exception and rely on callers to unpack the min completed number
            throw new ReplicationSinkException("Failed to process requests ending with seqNo: " + failedRequests, failedRequests);
        }
    }

    private boolean failedInPreviousBatch(Long seqNo) {
        return tracker.hasProcessed(seqNo) && tracker.hasPersisted(seqNo) == false;
    }

    private void handleDocumentFailure(Engine.Result result) {
        // document failure, just mark it done if there is an assigned seqno (it reached the engine)
        // so that we aren't left with gaps in our tracker.
        if (result.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            tracker.markSeqNoAsProcessed(result.getSeqNo());
            tracker.markSeqNoAsPersisted(result.getSeqNo());
        }
    }

    /**
     * Class wrapping an Operation indexed into the engine.
     */
    @PublicApi(since = "3.0.0")
    public abstract static class OperationDetails implements Comparable<OperationDetails> {
        private String docId;
        private long seqNo;
        private long primaryTerm;

        public OperationDetails(String docId, long seqNo, long primaryTerm) {
            this.docId = docId;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }

        public String docId() {
            return docId;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        @Override
        public int compareTo(OperationDetails o) {
            return Long.compare(this.seqNo, o.seqNo);
        }
    }

    /**
     * Index Operation
     */
    @PublicApi(since = "3.0.0")
    public static class IndexOperationDetails extends OperationDetails {

        private ParsedDocument parsedDoc;

        public IndexOperationDetails(String docId, long seqNo, long primaryTerm, ParsedDocument parsedDoc) {
            super(docId, seqNo, primaryTerm);
            this.parsedDoc = parsedDoc;
        }

        public ParsedDocument parsedDoc() {
            return parsedDoc;
        }
    }

    /**
     * Delete Operation
     */
    @PublicApi(since = "3.0.0")
    public static class DeleteOperationDetails extends OperationDetails {
        public DeleteOperationDetails(String docId, long seqNo, long primaryTerm) {
            super(docId, seqNo, primaryTerm);
        }
    }

    /**
     * A Sink that will receive batches of recently indexed operations.
     */
    @PublicApi(since = "3.0.0")
    public interface Sink {

        /**
         * @param shardId          {@link ShardId}
         * @param operationDetails {@link List< OperationDetails >}
         * @return seqNo up to which all operations successfully processed by the sink inclusive.
         */
        long acceptBatch(ShardId shardId, SortedSet<OperationDetails> operationDetails);

        default boolean supportsIndex(IndexSettings indexSettings) {
            return true;
        }
    }

    /**
     * Error that can return a seqNo to indicate partial failures from a sink.
     */
    static class ReplicationSinkException extends OpenSearchException {
        private final Set<Long> failed;

        public ReplicationSinkException(String message, Set<Long> failed) {
            super(message);
            this.failed = failed;
        }

        public Set<Long> getFailed() {
            return failed;
        }
    }
}
