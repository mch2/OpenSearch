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
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.node.NodeClosedException;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicationOperationListenerIT extends RemoteStoreBaseIntegTestCase {

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
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0ms")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        return plugins;
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
                addDocs(requestBuilder, threadID, 10);
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

    public void testMultiThreaded_10UniqueDocids_WithConcurrentUpdates() throws InterruptedException {
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
                addDocs(requestBuilder, 1, 10);
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
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 10);
        assertEquals(sink.getUniqueDocCount(), 10);
    }

    public void testConcurrentWritesWithPrimaryFailover() throws IOException, InterruptedException {
        createIndex("test", Settings.builder().put(indexSettings())
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build());
        internalCluster().startClusterManagerOnlyNode();
        ensureYellow("test");
        DiscoveryNode primaryNode = getNodeContainingPrimaryShard();
        String primary = primaryNode.getName();
        String replica = internalCluster().startDataOnlyNode();
        ensureGreen();
        int requestCount = randomIntBetween(2, 10);
        int batchSize = randomIntBetween(1, 100);
        final BulkResponse[] responses = new BulkResponse[requestCount];
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
                addDocs(requestBuilder, threadID, batchSize);
                // randomly delete something
                if (threadID > 0 && randomIntBetween(0, 10) == 1) {
                    requestBuilder.add(prepareDelete("val-" + (threadID-1)));
                }
                try {
                    responses[threadID] = requestBuilder.get();
                } catch (NodeClosedException nce) {
                    // do nothing we expect this could happen.
                }
                if (randomBoolean()) {
                    client().admin().indices().flush(request);
                }
            });
            threads[threadID].start();
        }
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        // index 10 more after failover
        BulkRequestBuilder requestBuilder = client().prepareBulk();
        addDocs(requestBuilder, requestCount, batchSize);
        BulkResponse bulk = requestBuilder.get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
            .setSize(10000)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndices("test").get();

        assertEquals(sink.getUniqueDocCount(), searchResponse.getHits().getTotalHits().value());

        Map<String, ParsedDocument> latestCopy = sink.getLatestCopy();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            ParsedDocument document = latestCopy.get(hit.getId());
            assertEquals(document.source(), hit.getSourceRef());
            logger.info("SOURCE MAPS {} {}", SourceLookup.sourceAsMap(document.source()), hit.getSourceAsMap());
        }
    }

    protected DiscoveryNode getNodeContainingPrimaryShard() {
        final ClusterState state = getClusterState();
        final ShardRouting primaryShard = state.routingTable().index("test").shard(0).primaryShard();
        return state.nodes().resolveNode(primaryShard.currentNodeId());
    }

    private void addDocs(BulkRequestBuilder requestBuilder, int offset, int count) {
        for (int i = offset * count; i < (offset * count) + count; i++) {
            Map<String, Object> sourceAsMap = new HashMap<>();
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                sourceAsMap.put("field-" + j, UUID.randomUUID().toString());
            }
            String id = "val-" + i;
            if (randomBoolean()) {
                requestBuilder.add(
                    prepareIndex(id, sourceAsMap)
                );
            } else {
                try {
                    // randomly do update vs insert
                    if (i > 0 && randomBoolean()) {
                        id = "val-" + (i-1);
                    }
                    requestBuilder.add(
                        client().prepareUpdate("test", id).setId(id)
                            .setUpsert(XContentFactory.jsonBuilder().startObject().field("bar", "baz").endObject())
                            .setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private IndexRequestBuilder prepareIndex(String id, Object... source) {
        return client().prepareIndex("test").setId(id).setSource(source);
    }

    private IndexRequestBuilder prepareIndex(String id, Map<String, ?> source) {
        return client().prepareIndex("test").setId(id).setSource(source);
    }

    private UpdateRequestBuilder prepareUpdate(String id, String field, String val) {
        return client().prepareUpdate("test", id).setDoc(Map.of(field, val));
    }


    private DeleteRequestBuilder prepareDelete(String id) {
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

        public Map<String, ParsedDocument> getLatestCopy() {
            return latestCopy;
        }

        // docs per batch processed
        private Map<Integer, List<ReplicationSink.OperationDetails>> ops = new HashMap<>();

        // all unique docs seen
        private Set<String> docs = new HashSet<>();

        // latest copy of each doc - can include deletes
        private Map<String, ParsedDocument> latestCopy = new HashMap<>();

        @Override
        public void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener) {
            System.out.println("THE FUCK");
            System.out.println(ops);
            if (operationDetails == null) {
                Assert.fail();
            }
            if (operationDetails.isEmpty()) {
                System.out.println("ITS EMPTY");
                Assert.fail();
            }
            ops.put(counter, operationDetails);
            for (OperationDetails operationDetail : operationDetails) {
                ParsedDocument document = null;
                if (operationDetail instanceof IndexingOperationDetails iod) {
                    document = iod.parsedDoc();
                    System.out.println("ADD: " + operationDetail.docId());
                    docs.add(operationDetail.docId());
                } else {
                    System.out.println("REMOVE: " + operationDetail.docId());
                    docs.remove(operationDetail.docId());
                }
                System.out.println("Doc id added " + operationDetail.docId());
                latestCopy.put(operationDetail.docId(), document);
            }
            counter++;
            listener.onResponse(operationDetails.stream().mapToLong(OperationDetails::seqNo).max().getAsLong());
        }
    }
}
