/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequestBuilder;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.update.UpdateRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicationOperationListenerIT extends OpenSearchIntegTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sink.clear();
    }

    static TestReplicationSink sink = new TestReplicationSink();

    public static class TestPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexingOperationSink(sink);
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestPlugin.class);
    }

    public void testUpdates() throws IOException {
        createIndex("test");
        ensureGreen();
        BulkResponse bulk = client().prepareBulk()
            .add(prepareIndex("multibulk1", "field1", "one")) // add
            .add(prepareIndex("multibulk1", "field2", "two")) // add new field
            .add(prepareUpdate("multibulk1", "field1", "three")) // update field 1
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));
        assertEquals(1, sink.getOps().size());
        ReplicationSink.OperationDetails operationDetails = sink.getOps().get(sink.getOps().keySet().stream().findFirst().get()).getFirst();
        assertEquals(ReplicationSink.IndexingOperationDetails.class, operationDetails.getClass());
        ReplicationSink.IndexingOperationDetails op = (ReplicationSink.IndexingOperationDetails) operationDetails;
        Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
        assertEquals(Map.of("field1", "three", "field2", "two"), sourceAsMap);

        GetResponse multibulk1 = client().prepareGet().setIndex("test").setId("multibulk1").setFetchSource(true).get();
        System.out.println("SOURCE IS");
        System.out.println(multibulk1.getSource());

        // for index-index, field1-field2 if diff field is added, will second request clobber first?
        // for index-update field1 and field2 will merge
    }

    private UpdateRequestBuilder prepareUpdate(String id, String field, String val) {
        return client().prepareUpdate("test", id).setDoc(Map.of(field, val));
    }

    public void testDeleteLast() throws IOException {
        createIndex("test");
        ensureGreen();
        BulkResponse bulk = client().prepareBulk()
            .add(prepareIndex("multibulk1", "field1", "one")) // add
            .add(prepareIndex("multibulk1", "field2", "two")) // add new field
            .add(prepareIndex("multibulk1", "field1", "three")) // update
            .add(prepareDelete("multibulk1"))
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());

        assertThat(refresh().getFailedShards(), equalTo(0));
    }

    public void testMultiThreaded_300UniqueDocs() throws InterruptedException {
        createIndex("test");
        ensureGreen();
        final BulkResponse[] responses = new BulkResponse[30];
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);
        Thread[] threads = new Thread[responses.length];
        FlushRequest request = new FlushRequest("test"); // <1>
        request.waitIfOngoing(true); // <1>
        request.force(true); // <1>
        for (int i = 0; i < responses.length; i++) {
            final int threadID = i;
            threads[threadID] = new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (Exception e) {
                    return;
                }
                BulkRequestBuilder requestBuilder = client().prepareBulk();
                try {
                    addDocs(requestBuilder, threadID, 10);
                } catch (IOException e) {
                    Assert.fail();
                }
                responses[threadID] = requestBuilder.get();
                if (randomBoolean()) {
                    client().admin().indices().flush(request);
                }
            });
            threads[threadID].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        client().admin().indices().prepareRefresh("test").get();
        for (BulkResponse response : responses) {
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
                Assert.fail();
            }
        }
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 300);
        assertEquals(sink.getUniqueDocCount(), 300);
    }

    public void testMultiThreaded_30UniqueDocs() throws InterruptedException {
        createIndex("test");
        ensureGreen();
        final BulkResponse[] responses = new BulkResponse[30];
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);
        Thread[] threads = new Thread[responses.length];
        FlushRequest request = new FlushRequest("test"); // <1>
        request.waitIfOngoing(true); // <1>
        request.force(true); // <1>
        for (int i = 0; i < responses.length; i++) {
            final int threadID = i;
            threads[threadID] = new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (Exception e) {
                    return;
                }
                BulkRequestBuilder requestBuilder = client().prepareBulk();
                try {
                    // index the same docs n times
                    addDocs(requestBuilder, 1, 10);
                } catch (IOException e) {
                    Assert.fail();
                }
                responses[threadID] = requestBuilder.get();
                if (randomBoolean()) {
                    client().admin().indices().flush(request);
                }
            });
            threads[threadID].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        client().admin().indices().prepareRefresh("test").get();
        for (BulkResponse response : responses) {
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
                Assert.fail();
            }
        }
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 30);
        assertEquals(sink.getUniqueDocCount(), 30);
    }

    public void testReplicaFailover() throws IOException {
        createIndex("test", Settings.builder().put(indexSettings()).put("index.number_of_replicas", 1).build());
        String primary = internalCluster().getDataNodeNames().stream().findFirst().get();
        String replica = internalCluster().startDataOnlyNode();
        ensureGreen();
        BulkRequestBuilder requestBuilder = client().prepareBulk();
        addDocs(requestBuilder, 1, 10);
        BulkResponse bulk = requestBuilder.get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));

        // index 10 more after failover
        requestBuilder = client().prepareBulk();
        addDocs(requestBuilder, 2, 10);
        bulk = requestBuilder.get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));

    }

    private void addDocs(BulkRequestBuilder requestBuilder, int offset, int count) throws IOException {
        for (int i = offset * count; i < (offset * count) + count; i++) {
            logger.info("Indexing {}", offset);
            requestBuilder.add(
                prepareIndex("val-" + i, "field1", "val-" + i)
            );
        }
    }


    private IndexRequestBuilder prepareIndex(String id, String field, String val) throws IOException {
        return client().prepareIndex("test").setId(id).setSource(field, val);
    }

    private DeleteRequestBuilder prepareDelete(String id) throws IOException {
        return client().prepareDelete("test", id);
    }

    static class TestReplicationSink implements ReplicationSink {
        int counter = 0;

        public void clear() {
            this.ops.clear();
            this.docs.clear();
        }

        // counter of gen with list of ops
        public Map<Integer, List<OperationDetails>> getOps() {
            return ops;
        }

        public int getUniqueDocCount() {
            return docs.size();
        }

        private Map<Integer, List<ReplicationSink.OperationDetails>> ops = new HashMap<>();
        private Set<String> docs = new HashSet<>();


        @Override
        public void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener) {
//            if (operationDetails == null) {
//                Assert.fail();
//            }
//            ops.put(counter, operationDetails);
//            for (OperationDetails operationDetail : operationDetails) {
//                docs.add(operationDetail.docId());
//            }
//            counter++;
//            listener.onResponse(operationDetails.stream().mapToLong(OperationDetails::seqNo).max().getAsLong());
        }
    }
}
