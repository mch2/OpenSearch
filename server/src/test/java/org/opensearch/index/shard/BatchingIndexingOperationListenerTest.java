/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.index.shard.BatchingIndexingOperationListener.OperationDetails;

import java.io.IOException;
import java.util.List;

public class BatchingIndexingOperationListenerTest extends IndexShardTestCase {

    private IndexShard indexShard;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newStartedShard(true); // Primary shard
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

        BatchingIndexingOperationListener batchListener = indexShard.getBatchListener();
        List<OperationDetails> operationDetails = batchListener.pollUntil(2L);
        assertEquals(1, operationDetails.size());
        assertTrue(operationDetails.get(0).isDelete());
    }

    public void testThreeOperationsForSameDocId() throws Exception {
        final String docId = "doc1";

        // First index operation - Initial document - seqNo 0
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value1\",\"field2\":\"value2\"}");

        // Second index operation - Update some fields - seqNo 1
        indexDoc(indexShard, "_doc", docId, "{\"field1\":\"value3\"}");

        // Third operation - Delete the document - seqNo 2
        deleteDoc(indexShard, docId);

        BatchingIndexingOperationListener batchListener = indexShard.getBatchListener();

        batchListener.addListener(2L, (e) -> {
            if (e != null) {
                logger.error("What", e);
                Assert.fail();
            }
        });
        assertEquals(2, batchListener.getSeenCheckpoint());
        assertEquals(2, batchListener.getUploadedCheckpoint());
    }
}
