/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.Term;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.InternalEngineTests;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.BatchIndexingOperationListener.OperationDetails;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.search.lookup.SourceLookup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class BatchIndexingOperationListenerIndexShardTests extends IndexShardTestCase {

    private IndexShard indexShard;
    private BatchIndexingOperationListener listener;
    TestSink testSink;
    ShardRouting shardRouting;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testSink = new TestSink();
        shardRouting = TestShardRouting.newShardRouting(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        listener = new BatchIndexingOperationListener(
            shardRouting.shardId(),
            Set.of(testSink),
            threadPool,
            TimeValue.timeValueMillis(5000),
            DefaultRemoteStoreSettings.INSTANCE
        );
        indexShard = newStartedShard(p -> newShard(p, listener), true);
    }

    @After
    public void afterTest() throws IOException {
        closeShards(indexShard);
    }

    public void testDocumentFailure_IndexOperation() {
        Engine.Index doc = buildIndexRequest("doc");
        long seqNo = 0L;

        // doesn't reach engine so no seqNo
        Engine.IndexResult r1 = new Engine.IndexResult(new RuntimeException("Failed"), doc.version());
        listener.postIndex(indexShard.shardId, doc, r1);

        // reached the engine so has assigned seqNo
        Engine.IndexResult r2 = new Engine.IndexResult(new RuntimeException("Failed"), doc.version(), seqNo, seqNo);
        listener.postIndex(indexShard.shardId, doc, r2);
        assertTrue(listener.hasProcessed(seqNo));
        assertTrue(listener.hasCompleted(seqNo));
        // nothing received by sink
        assertEquals(Collections.emptySet(), testSink.operationDetails);
    }

    public void testDocumentFailure_DeleteOperation() {
        Engine.Delete doc = buildDeleteRequest("doc");
        long seqNo = 0L;

        Engine.DeleteResult r2 = new Engine.DeleteResult(new RuntimeException("Failed"), doc.version(), doc.primaryTerm());
        listener.postDelete(indexShard.shardId, doc, r2);

        Engine.DeleteResult r1 = new Engine.DeleteResult(new RuntimeException("Failed"), doc.version(), doc.primaryTerm(), seqNo, true);
        listener.postDelete(indexShard.shardId, doc, r1);

        assertTrue(listener.hasProcessed(seqNo));
        assertTrue(listener.hasCompleted(seqNo));
        // nothing received by sink
        assertEquals(Collections.emptySet(), testSink.operationDetails);
    }

    public void testDedupeOnSameDocId_DeleteLast() throws IOException {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        // Third operation - Delete the document - seqNo 2
        Engine.DeleteResult deleteResult = deleteDoc(indexShard, docId);

        indexShard.waitForBatchCompletion(new TreeSet<>(Collections.singleton(deleteResult.getSeqNo())), (e) -> {
            assertEquals(Set.of(deleteResult.getSeqNo()), testSink.lastestReceivedSequenceNumbers());
        });
    }

    public void testDedupeOnSameDocId_Index_Index() throws IOException {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        Engine.IndexResult doc = indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        indexShard.waitForBatchCompletion(new TreeSet<>(Collections.singleton(doc.getSeqNo())), (e) -> {
            assertEquals(Set.of(doc.getSeqNo()), testSink.lastestReceivedSequenceNumbers());
            SortedSet<OperationDetails> operationDetails = testSink.getOperationDetails();
            BatchIndexingOperationListener.IndexOperationDetails op =
                (BatchIndexingOperationListener.IndexOperationDetails) operationDetails.first();
            assertEquals(op.getClass(), BatchIndexingOperationListener.IndexOperationDetails.class);
            assertEquals(docId, op.docId());
            Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
            assertEquals(Map.of("field1", "value3"), sourceAsMap);
        });
    }

    public void testDedupeOnSameDocId_Index_Update() throws IOException {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        Engine.IndexResult doc = indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\",\"field2\":\"value2\"}");

        indexShard.waitForBatchCompletion(new TreeSet<>(Collections.singleton(doc.getSeqNo())), (e) -> {
            SortedSet<OperationDetails> operationDetails = testSink.getOperationDetails();
            assertEquals(1, operationDetails.size());
            BatchIndexingOperationListener.IndexOperationDetails op =
                (BatchIndexingOperationListener.IndexOperationDetails) operationDetails.first();
            assertEquals(op.getClass(), BatchIndexingOperationListener.IndexOperationDetails.class);
            assertEquals(docId, op.docId());
            Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
            assertEquals(Map.of("field1", "value3", "field2", "value2"), sourceAsMap);
        });
    }

    public void testDedupeOnSameDocId_AcrossRequestsInSameBatch() throws InterruptedException, IOException {
        String docId = "mydoc";
        indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId);
        indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId, "{\"field1\":\"value1\"}");

        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(new TreeSet<>(Set.of(0L)), true);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(new TreeSet<>(Set.of(1L)), true);
        waitAndAssert(List.of(r1, r2));

        assertTrue(listener.hasProcessed(0L));
        assertTrue(listener.hasProcessed(1L));
        assertEquals(Set.of(1L), testSink.lastestReceivedSequenceNumbers());
        assertEquals(1L, listener.getSeenCheckpoint());
        assertEquals(1L, listener.getCompletedCheckpoint());
    }

    public void testDedupeOnSameDocId_AcrossRequestsInSameBatchWithPartialFailure() throws InterruptedException, IOException {
        // in this case we have a dedupe in the batch and a failure occurs, in this instance we
        // fail all reqs that were part of the batch, even if they don't include that deduped operation.
        String docId = "mydoc";
        Engine.IndexResult doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId);// seqno 0
        assertEquals(0L, doc.getSeqNo());
        doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId, "{\"field1\":\"1L\"}");// seqno 1 dedupe
        assertEquals(1L, doc.getSeqNo());
        testSink.setFailureAfter(0L);

        // 0 gets deduped by 1L, but both ops come from separate requests.
        // deduplication occurs *before* we hand off to the sink
        // in this case we expect *both* requests to fail because the deduped op can not be ack'd.
        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(new TreeSet<>(Set.of(0L)), false);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(new TreeSet<>(Set.of(1L)), false);
        waitAndAssert(List.of(r1, r2));

        assertTrue(listener.hasProcessed(0L));
        assertEquals(Set.of(1L), testSink.lastestReceivedSequenceNumbers());
        assertEquals(1L, listener.getSeenCheckpoint());
        assertEquals(1L, listener.getCompletedCheckpoint());
    }

    public void testDedupeOnSameDocId_WithFailureAndDedupe_FirstRequestArrivesLast() throws Exception {
        // this is the same as the previous test but with additional docs per request, and r2 is finished before r1
        // r1
        indexDoc(0L);
        String docId = "mydoc";
        Engine.IndexResult doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId);// seqno 1
        assertEquals(1L, doc.getSeqNo());
        indexDoc(2L);

        // r2
        indexDoc(3L);
        doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId, "{\"field1\":\"1234\"}");// seqno 4 dedupe
        assertEquals(4L, doc.getSeqNo());
        indexDoc(5L);

        testSink.setFailureAfter(3L);

        // 1L (R1) is deduped by 4L (R2), R2 is processed first.
        // in this case we expect *both* requests to fail, but r1 is received after r2
        waitAndAssert(Set.of(3L, 4L, 5L), e -> assertNotNull("R2 [3, 4, 5] should fail", e)); // r2
        // because 1 is never passed to the sink, it will never be marked completed until r2 is received
        assertEquals("1L is not yet completed", 0L, listener.getCompletedCheckpoint());
        assertFalse("1L has not been completed", listener.hasCompleted(1L));
        assertTrue("3L has been completed", listener.hasCompleted(3L));
        assertTrue("4L has been completed", listener.hasCompleted(4L));

        assertEquals(5L, listener.getSeenCheckpoint());
        waitAndAssert(Set.of(0L, 1L, 2L), e -> assertNotNull("R1 [0, 1, 2] should fail", e)); // r1

        assertTrue(listener.hasProcessed(0L));
        assertEquals(Set.of(0L, 2L, 3L, 4L, 5L), testSink.lastestReceivedSequenceNumbers());
        assertEquals(5L, listener.getSeenCheckpoint());
        assertEquals(
            "Even with failure, every failed seqNo is part of a received request, so this advances",
            5L,
            listener.getCompletedCheckpoint()
        );
        assertEquals(5L, listener.getCompletedCheckpoint());
        assertEquals(5L, listener.getSeenCheckpoint());
    }

    public void testDedupeOnSameDocId_WithNoFailure_FirstRequestArrivesLast() throws Exception {
        // this is the same as the previous test but with additional docs per request, and r2 is finished before r1
        // r1
        indexDoc(0L);
        String docId = "mydoc";
        Engine.IndexResult doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId);// seqno 1
        assertEquals(1L, doc.getSeqNo());
        indexDoc(2L);

        // r2
        indexDoc(3L);
        doc = indexDoc(indexShard, MediaTypeRegistry.JSON.type(), docId, "{\"field1\":\"1234\"}");// seqno 4 dedupe
        assertEquals(4L, doc.getSeqNo());
        indexDoc(5L);

        // 1L (R1) is deduped by 4L (R2), R2 is processed first.
        // in this case we expect *both* requests to pass, but r1 is received after r2
        waitAndAssert(Set.of(3L, 4L, 5L), e -> assertNull("R2 [3, 4, 5] should not fail", e)); // r2
        // because 1 is never passed to the sink, it will never be marked completed until r2 is received
        assertEquals(5L, listener.getCompletedCheckpoint());
        assertEquals(5L, listener.getSeenCheckpoint());

        // assertEquals(5L, listener.getSeenCheckpoint());
        waitAndAssert(Set.of(0L, 1L, 2L), e -> assertNull("R1 [0, 1, 2] should not fail", e)); // r1
    }

    public void testSinkThrowsRandomException() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);
        // 0, 1, 2, 3 are all in the queue when the listener arrives

        // mark ops after 1L as failed
        testSink.setFailureAfter(1L, new RuntimeException("Failed"));

        TreeSet<Long> r1 = new TreeSet<>(Set.of(0L, 2L));
        final TreeSet<Long> expectedSeqNosSentToSink = new TreeSet<>(Set.of(0L, 1L, 2L));
        waitAndAssert(r1, (e) -> {
            assertNotNull(e);
            assertEquals(expectedSeqNosSentToSink, testSink.lastestReceivedSequenceNumbers());
            assertEquals(3, testSink.operationDetails.size());
            assertEquals(2L, listener.getSeenCheckpoint());
        });

        // assert all ops in first req were marked persisted, but completed cp has
        // not passed 1L
        assertEquals("First req failed, all of its seqNos are marked persisted", 0L, listener.getCompletedCheckpoint());
        assertEquals(2L, listener.getSeenCheckpoint());

        TreeSet<Long> r2 = new TreeSet<>(Set.of(1L, 3L));
        // request fails, we received an exception from the sink
        waitAndAssert(r2, Assert::assertNotNull);
        // nothing sent to the sink in second req, first batch fails which included 1L, so we don't bother sending 3L.
        assertEquals(expectedSeqNosSentToSink, testSink.lastestReceivedSequenceNumbers());
        assertTrue(listener.hasCompleted(0L));
        assertTrue(listener.hasCompleted(1L));
        assertTrue(listener.hasCompleted(2L));
        assertTrue(listener.hasCompleted(3L));
    }

    public void testFailureAcrossRequests_secondRequestSucceeds() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);
        // 0, 1, 2, 3 are all in the queue when the listener arrives

        // mark ops after 1L as failed
        testSink.setFailureAfter(1L);

        TreeSet<Long> r1 = new TreeSet<>(Set.of(0L, 2L));
        waitAndAssert(r1, (e) -> {
            assertNotNull(e);
            assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L)), testSink.lastestReceivedSequenceNumbers());
            assertEquals(3, testSink.operationDetails.size());
            assertEquals(2L, listener.getSeenCheckpoint());
        });

        // assert all ops in first req were marked persisted, but completed cp has
        // not passed 1L
        assertEquals(2L, listener.getCompletedCheckpoint());
        assertEquals(2L, listener.getSeenCheckpoint());

        TreeSet<Long> r2 = new TreeSet<>(Set.of(1L, 3L));
        // request succeeds because 1 was successful in first round and we have not yet processed 3.
        waitAndAssert(r2, Assert::assertNull);
        // only 3 processed in 2nd round
        assertEquals(new TreeSet<>(Set.of(3L)), testSink.lastestReceivedSequenceNumbers());
        assertTrue(listener.hasCompleted(0L));
        assertTrue(listener.hasCompleted(1L));
        assertTrue(listener.hasCompleted(2L));
    }

    public void testFailureAcrossRequests_secondRequestFails() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);

        // 0, 1, 2, 3 are all in the queue when the listener arrives
        // waiting for seqNo 2.

        // mark failure after 1L as failed
        testSink.setFailureAfter(1L);

        TreeSet<Long> r1 = new TreeSet<>(Set.of(0L, 3L));
        waitAndAssert(r1, (e) -> {
            assertNotNull(e);
            assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L, 3L)), testSink.lastestReceivedSequenceNumbers());
        });

        assertEquals(1L, listener.getCompletedCheckpoint());
        assertEquals(3L, listener.getSeenCheckpoint());

        TreeSet<Long> r2 = new TreeSet<>(Set.of(1L, 2L));
        // request fails because the first batch failed after 1.
        waitAndAssert(r2, Assert::assertNotNull);
        // no new batch was received
        assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L, 3L)), testSink.lastestReceivedSequenceNumbers());
    }

    public void testFailureAcrossRequests_SingleBatch_oneReqSucceeds() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);
        // mark failure after 3 as failed, this should fail 5
        testSink.setFailureAfter(1L);

        // both requests are part of the same batch
        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(new TreeSet<>(Set.of(0L, 1L)), true);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(new TreeSet<>(Set.of(2L, 3L)), false);
        waitAndAssert(List.of(r1, r2));
        assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L, 3L)), testSink.lastestReceivedSequenceNumbers());
    }

    public void testFailureAcrossRequests_singleBatch_partialSuccess() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);
        // mark failure after 1 as failed, this should fail 2 and 3
        testSink.setFailureAfter(1L);

        // all requests are part of the same batch
        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(new TreeSet<>(Set.of(0L)), true);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(new TreeSet<>(Set.of(1L)), true);
        Tuple<Set<Long>, Boolean> r3 = new Tuple<>(new TreeSet<>(Set.of(2L)), false);
        Tuple<Set<Long>, Boolean> r4 = new Tuple<>(new TreeSet<>(Set.of(3L)), false);
        waitAndAssert(List.of(r1, r2, r3, r4));
        assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L, 3L)), testSink.lastestReceivedSequenceNumbers());
    }

    public void testFailureAcrossRequests_WithMoreDocumentsInBetweenBatches() throws InterruptedException {
        LongStream.range(0, 5).forEach(this::indexDoc);

        testSink.setFailureAfter(1L);

        // r1 waits on 0 and 3
        waitAndAssert(new TreeSet<>(Set.of(0L, 3L)), (e) -> {
            assertNotNull(e);
            assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L, 3L)), testSink.lastestReceivedSequenceNumbers());
            assertEquals(4, testSink.operationDetails.size());
            assertEquals(3L, listener.getSeenCheckpoint());
        });

        LongStream.range(5, 10).forEach(this::indexDoc);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(Set.of(1L, 2L, 4L), false); // should fail - 2L-4L failed in first batch.
        Tuple<Set<Long>, Boolean> r3 = new Tuple<>(Set.of(5L, 6L, 7L, 8L), true); // should succeed
        Tuple<Set<Long>, Boolean> r4 = new Tuple<>(Set.of(9L), true); // should succeed
        waitAndAssert(List.of(r2, r3, r4));
        // this omits 4L, as it will be part of our previously failed req.
        assertEquals(
            "Sink should not receive seqNos of previously failed requests",
            Set.of(5L, 6L, 7L, 8L, 9L),
            testSink.lastestReceivedSequenceNumbers()
        );
    }

    public void testFailureAcrossRequests_ThreeBatches() throws InterruptedException {
        LongStream.range(0, 5).forEach(this::indexDoc);

        testSink.setFailureAfter(1L);

        // r1 waits on 0 and 3
        // failed
        waitAndAssert(new TreeSet<>(Set.of(4L)), Assert::assertNotNull); // fails 3-5L

        waitAndAssert(Set.of(0L), true); // succeeds
        waitAndAssert(Set.of(3L), false);
        waitAndAssert(Set.of(1L), true); // succeeds
        waitAndAssert(Set.of(2L), false); // fails
    }

    public void testMultipleBatchesToProcessor_FirstCompletesAllOps() throws InterruptedException {
        LongStream.range(0, 5).forEach(this::indexDoc);

        // first batch completes all writes 0-4L.
        waitAndAssert(List.of(new Tuple<>(new TreeSet<>(Set.of(0L, 4L)), true)));

        // index 5L-9L
        LongStream.range(5, 10).forEach(this::indexDoc);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(Set.of(1L, 2L, 4L), true);
        Tuple<Set<Long>, Boolean> r3 = new Tuple<>(Set.of(5L, 6L, 7L, 8L), true);
        Tuple<Set<Long>, Boolean> r4 = new Tuple<>(Set.of(9L), true);
        waitAndAssert(List.of(r2, r3, r4));
    }

    public void testMultipleBatchesToProcessor_operationsNeverArrive_blockingqueueReturnsNull() throws InterruptedException, IOException {
        listener = new BatchIndexingOperationListener(
            shardRouting.shardId(),
            Set.of(testSink),
            threadPool,
            TimeValue.timeValueMillis(1),
            DefaultRemoteStoreSettings.INSTANCE
        );
        closeShards(indexShard);
        indexShard = newStartedShard(p -> newShard(p, listener), true);
        LongStream.range(0, 1).forEach(this::indexDoc); // only insert two ops
        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(Set.of(0L, 1L, 2L, 7L, 8L), false);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(Set.of(3L, 4L, 5L), false);
        waitAndAssert(List.of(r1, r2));
    }

    public void testMultipleBatchesToProcessor_operationsNeverArrive_WithGaps() throws InterruptedException, IOException {
        listener = new BatchIndexingOperationListener(
            shardRouting.shardId(),
            Set.of(testSink),
            threadPool,
            TimeValue.timeValueMillis(1),
            DefaultRemoteStoreSettings.INSTANCE
        );
        closeShards(indexShard);
        indexShard = newStartedShard(p -> newShard(p, listener), true);
        LongStream.range(0, 5).forEach(this::simulatePostIndex);
        LongStream.range(6, 9).forEach(this::simulatePostIndex);
        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(Set.of(0L, 1L, 2L, 6L, 7L, 8L), false); // r1 fails because 0,1,2 are pulled from the
                                                                                           // queue
        // when r2 listener is added but but never persisted because of interrupt.
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(Set.of(3L, 4L, 5L), false);
        waitAndAssert(List.of(r2));
        simulatePostIndex(5); // 5 finally arrives
        waitAndAssert(List.of(r1));
    }

    public void testMultipleBatchesToProcessor_operationsOutOfOrderPollHigherThanRequired() throws InterruptedException, IOException {
        listener = new BatchIndexingOperationListener(
            shardRouting.shardId(),
            Set.of(testSink),
            threadPool,
            TimeValue.timeValueMillis(1000),
            DefaultRemoteStoreSettings.INSTANCE
        );
        closeShards(indexShard);
        indexShard = newStartedShard(p -> newShard(p, listener), true);
        LongStream.range(0, 5).forEach(this::simulatePostIndex);
        LongStream.range(6, 9).forEach(this::simulatePostIndex);
        simulatePostIndex(9); // higher than our required two batches
        simulatePostIndex(10); // higher than our required two batch

        Tuple<Set<Long>, Boolean> r1 = new Tuple<>(Set.of(0L, 1L, 2L, 6L, 7L, 8L), false);
        Tuple<Set<Long>, Boolean> r2 = new Tuple<>(Set.of(3L, 4L, 5L), false);
        waitAndAssert(List.of(r2));
        assertFalse(listener.hasProcessed(6L));
        assertFalse(listener.hasProcessed(7L));
        assertFalse(listener.hasProcessed(8L));
        assertFalse(listener.hasProcessed(9));
        assertFalse(listener.hasProcessed(10));
        simulatePostIndex(5); // 5 shows up
        waitAndAssert(List.of(r1));
        assertTrue(listener.hasProcessed(6L));
        assertTrue(listener.hasProcessed(7L));
        assertTrue(listener.hasProcessed(8L));
        assertFalse(listener.hasProcessed(9));
        assertFalse(listener.hasProcessed(10));
        waitAndAssert(Set.of(9L, 10L), true);
        assertTrue(listener.hasProcessed(9));
        assertTrue(listener.hasProcessed(10));
    }

    private void simulatePostIndex(long seqNo) {
        Engine.IndexResult response = new Engine.IndexResult(1L, 1L, seqNo, true);
        listener.postIndex(indexShard.shardId, buildIndexRequest("0L"), response);
    }

    public void testFailureAcrossRequests_AllSeqNosInBatchAlreadyFailed() throws InterruptedException {
        LongStream.range(0, 4).forEach(this::indexDoc);
        // set first batch to poll until 3L, but fail after 1L
        testSink.setFailureAfter(0L);

        // r1 waits on 0 and 3
        waitAndAssert(new TreeSet<>(Set.of(0L, 2L)), (e) -> {
            assertNotNull(e);
            assertEquals(new TreeSet<>(Set.of(0L, 1L, 2L)), testSink.lastestReceivedSequenceNumbers());
            assertEquals(3, testSink.operationDetails.size());
            assertEquals(2L, listener.getSeenCheckpoint());
            BatchIndexingOperationListener.SinkException rse = (BatchIndexingOperationListener.SinkException) e;
        });

        assertEquals(0L, listener.getCompletedCheckpoint());
        waitAndAssert(new TreeSet<>(Set.of(1L)), Assert::assertNotNull);
    }

    // wait and assert a single request in the batch
    private void waitAndAssert(Set<Long> seqNos, Boolean succeeded) throws InterruptedException {
        waitAndAssert(List.of(new Tuple<>(seqNos, succeeded)));
    }

    // wait and assert multiple reqs in a single batch with extra assertions in the callback.
    private void waitAndAssert(Set<Long> seqNos, Consumer<Exception> assertions) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        indexShard.waitForBatchCompletion(new TreeSet<>(seqNos), (e) -> {
            assertions.accept(e);
            latch.countDown();
        });
        latch.await(10, TimeUnit.SECONDS);
    }

    // wait and assert multiple reqs in a single batch
    private void waitAndAssert(List<Tuple<Set<Long>, Boolean>> list) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(list.size());
        for (Tuple<Set<Long>, Boolean> tuple : list) {
            TreeSet<Long> req = new TreeSet<>(tuple.v1());
            indexShard.waitForBatchCompletion(req, (e) -> {
                if (tuple.v2()) {
                    assertNull("Expected request [" + tuple.v1() + "] to succeed", e);
                } else {
                    assertNotNull("Expected request [" + tuple.v1() + "] to fail", e);
                }
                latch.countDown();
            });
        }
        latch.await(10, TimeUnit.SECONDS);
    }

    private void indexDoc(long seqNo) {
        try {
            indexDoc(indexShard, MediaTypeRegistry.JSON.type(), String.valueOf(seqNo), "{\"field1\":" + seqNo + "}");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Engine.Index buildIndexRequest(String id) {
        ParsedDocument doc = InternalEngineTests.createParsedDoc(id, null);
        return new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
    }

    private Engine.Delete buildDeleteRequest(String id) {
        Term term = new Term("_id", Uid.encodeId(id));
        return new Engine.Delete("_id", term, randomNonNegativeLong());
    }

    class TestSink implements BatchIndexingOperationListener.Sink {
        private long failAfterSeqNo = SequenceNumbers.NO_OPS_PERFORMED;

        private SortedSet<OperationDetails> operationDetails = new TreeSet<>();
        private RuntimeException toThrow;

        public SortedSet<OperationDetails> getOperationDetails() {
            return operationDetails;
        }

        public Set<Long> lastestReceivedSequenceNumbers() {
            return operationDetails.stream()
                .map(BatchIndexingOperationListener.OperationDetails::seqNo)
                .collect(Collectors.toCollection(TreeSet::new));
        }

        public void setFailureAfter(long seqNo) {
            failAfterSeqNo = seqNo;
        }

        public void setFailureAfter(long seqNo, RuntimeException toThrow) {
            failAfterSeqNo = seqNo;
            this.toThrow = toThrow;
        }

        @Override
        public long acceptBatch(ShardId shardId, SortedSet<OperationDetails> operationDetails) {
            /// assert all received doc Ids are unique:
            assertEquals(
                "Sink should not be handed duplicates by docId",
                operationDetails.size(),
                operationDetails.stream().map(BatchIndexingOperationListener.OperationDetails::docId).collect(Collectors.toSet()).size()
            );
            assertEquals(
                "Sink should not be handed duplicates by seqNo",
                operationDetails.size(),
                operationDetails.stream().map(BatchIndexingOperationListener.OperationDetails::seqNo).collect(Collectors.toSet()).size()
            );
            // fail only if we've set a seqNo and its part of the incoming batch.
            this.operationDetails = operationDetails;
            if (failAfterSeqNo != SequenceNumbers.NO_OPS_PERFORMED) {
                long failAfter = failAfterSeqNo;
                failAfterSeqNo = SequenceNumbers.NO_OPS_PERFORMED;

                if (toThrow != null) {
                    RuntimeException e = toThrow;
                    toThrow = null;
                    throw e;
                }
                return failAfter;
            } else {
                return operationDetails.stream()
                    .mapToLong(BatchIndexingOperationListener.OperationDetails::seqNo)
                    .max()
                    .orElse(SequenceNumbers.NO_OPS_PERFORMED);
            }
        }
    }
}
