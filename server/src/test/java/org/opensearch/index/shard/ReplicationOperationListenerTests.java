/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ReplicationSink.OperationDetails;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: IGNORE THESE TESTS FOR NOW - I NEED TO UPDATE INDEXSHARDTESTCASE TO TAKE A LIST OF SINKS.
// SEE ITs.
@LuceneTestCase.AwaitsFix(bugUrl = "")
public class ReplicationOperationListenerTests extends IndexShardTestCase {


    private IndexShard indexShard;
    private ReplicationOperationListener batchListener;
    TestReplicationSink testSink;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testSink = new TestReplicationSink();
//        batchListener = new ReplicationOperationListener(indexShard, List.of(testSink), threadPool.getThreadContext());
        indexShard = newStartedShard(p -> newShard(p, batchListener), true);
    }

    @After
    public void afterTest() throws IOException {
        closeShards(indexShard);
    }

    public void testReduce() throws IOException {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        // Third operation - Delete the document - seqNo 2
        deleteDoc(indexShard, docId);

        List<OperationDetails> operationDetails = batchListener.pollUntil(2L);
        assertEquals(1, operationDetails.size());
    }

    public void testThreeOperationsForSameDocId() throws Exception {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        // Third operation - Delete the document - seqNo 2
        deleteDoc(indexShard, docId);

        batchListener.addListener(2L, (e) -> {
            if (e != null) {
                logger.error("What", e);
                Assert.fail();
            }
        });
        assertEquals(2, batchListener.getSeenCheckpoint());
        assertEquals(2, batchListener.getReplicatedCheckpoint());
    }

    public void testMergeUpdate() throws Exception {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        batchListener.addListener(1L, (e) -> {
            if (e != null) {
                logger.error("Failure not expected", e);
                Assert.fail("Failure not expected");
            }
        });
        List<OperationDetails> operationDetails = testSink.getOperationDetails();
        assertEquals(1, operationDetails.size());
        assertTrue(operationDetails.get(0) instanceof ReplicationSink.IndexingOperationDetails);
        ReplicationSink.IndexingOperationDetails op = (ReplicationSink.IndexingOperationDetails) operationDetails.get(0);
        assertEquals(docId, op.docId());
        Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
        assertEquals(Map.of("field1", "value3", "field2", "value2"), sourceAsMap);
        assertEquals(1, batchListener.getSeenCheckpoint());
        assertEquals(1, batchListener.getReplicatedCheckpoint());
    }

    public void testMergeUpdateReverse() throws Exception {
        final String docId = "doc1";

        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        batchListener.addListener(1L, (e) -> {
            if (e != null) {
                logger.error("Failure not expected", e);
                Assert.fail("Failure not expected");
            }
        });
        List<OperationDetails> operationDetails = testSink.getOperationDetails();
        assertEquals(1, operationDetails.size());
        assertTrue(operationDetails.get(0) instanceof ReplicationSink.IndexingOperationDetails);
        ReplicationSink.IndexingOperationDetails op = (ReplicationSink.IndexingOperationDetails) operationDetails.get(0);
        assertEquals(docId, op.docId());
        Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
        assertEquals(Map.of("field1", "value1", "field2", "value2"), sourceAsMap);
        assertEquals(1, batchListener.getSeenCheckpoint());
        assertEquals(1, batchListener.getReplicatedCheckpoint());
    }

    class TestReplicationSink implements ReplicationSink {
        private List<OperationDetails> operationDetails = new ArrayList<>();

        public List<OperationDetails> getOperationDetails() {
            return operationDetails;
        }

        @Override
        public void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener) {
            this.operationDetails = operationDetails;
            listener.onResponse(operationDetails.stream().mapToLong(OperationDetails::seqNo).max().getAsLong());
        }
    }

    ;
}
