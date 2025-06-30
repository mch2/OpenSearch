/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.search.SearchHit;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_BATCH_OPERATION_LISTENER_BUFFER_INTERVAL_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_BATCH_OPERATION_LISTENER_DRAIN_TIMEOUT_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_BATCH_OPERATION_LISTENER_POLL_TIMEOUT_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BatchIndexingOperationListenerIT extends RemoteStoreBaseIntegTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sink.clear();
    }

    static TestSink sink = new TestSink();
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    public static class TestPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexEventListener(sink);
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
        BatchIndexingOperationListener.OperationDetails operationDetails = sink.getOps()
            .get(sink.getOps().keySet().stream().findFirst().get())
            .stream()
            .findFirst()
            .get();
        assertEquals(BatchIndexingOperationListener.UpdateOperationDetails.class, operationDetails.getClass());
        assertEquals(
            Map.of("field1", "three"),
            ((BatchIndexingOperationListener.UpdateOperationDetails) operationDetails).getSourceAsMap()
        );
    }

    public void testUpdates_nestedObject() throws IOException {
        createIndex("test");
        ensureGreen();
        Map<String, Object> field1Object = Map.of("subfield1", "subvalue1", "subfield2", "subvalue2");
        Map<String, Object> updateObject = Map.of("subfield1", "subvalue3");

        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex("test").setId("doc1").setSource("field1", field1Object)) // add
            .add(client().prepareUpdate("test", "doc1").setDoc("field1", updateObject)) // update field1
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));
        assertEquals(1, sink.getOps().size());
        BatchIndexingOperationListener.OperationDetails operationDetails = sink.getOps()
            .get(sink.getOps().keySet().stream().findFirst().get())
            .stream()
            .findFirst()
            .get();
        assertEquals(BatchIndexingOperationListener.UpdateOperationDetails.class, operationDetails.getClass());
        BatchIndexingOperationListener.UpdateOperationDetails op = (BatchIndexingOperationListener.UpdateOperationDetails) operationDetails;
        Map<String, Object> expected = Map.of("subfield1", "subvalue3");
        assertEquals(Map.of("field1", expected), op.getSourceAsMap());
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
        FlushRequest request = new FlushRequest("test");
        request.waitIfOngoing(true);
        request.force(true);
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

    public void testConcurrentWritesWithPrimaryStopped() throws Exception {
        createIndex("test", Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
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
        FlushRequest request = new FlushRequest("test");
        request.waitIfOngoing(true);
        request.force(true);
        Set<String> docIds = new HashSet<>();
        for (int i = 0; i < responses.length; i++) {
            final int threadID = i;
            threads[threadID] = new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (Exception e) {
                    return;
                }
                BulkRequestBuilder requestBuilder = client(replica).prepareBulk();
                docIds.addAll(addDocs(requestBuilder, threadID, batchSize));
                // randomly delete something
                if (threadID > 0 && randomIntBetween(0, 10) == 1) {
                    String id = "val-" + (threadID - 1);
                    docIds.remove(id);
                    requestBuilder.add(prepareDelete(id));
                }
                BulkResponse bulkItemResponses = requestBuilder.get();
                bulkItemResponses.forEach(bulkItemResponse -> {
                    if (bulkItemResponse.getFailure() != null) {
                        logger.error(bulkItemResponse.getFailureMessage());
                    }
                });
                responses[threadID] = bulkItemResponses;
                if (randomBoolean()) {
                    client().admin().indices().flush(request);
                }
            });
            threads[threadID].start();
        }

        // wait until sink has something in it from old primary
        assertBusy(() -> assertTrue(sink.counter > 0));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards("test");

        // index 10 more after failover
        BulkRequestBuilder requestBuilder = client().prepareBulk();
        addDocs(requestBuilder, requestCount, batchSize);
        BulkResponse bulk = requestBuilder.get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        client().admin().indices().prepareFlush("test").setForce(true).get();

        assertThat(refresh().getFailedShards(), equalTo(0));
        SearchResponse searchResponse = client().prepareSearch()
            .setSize(10000)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndices("test")
            .seqNoAndPrimaryTerm(true)
            .get();

        logger.info("Sink'd {}", sink.docs);
        assertEquals("Sink count does not match hit count", sink.getUniqueDocCount(), searchResponse.getHits().getTotalHits().value);

        // ensure the sink has an entry for each indexed document.
        Map<String, BatchIndexingOperationListener.IndexOperationDetails> latestCopy = sink.getLatestCopy();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BatchIndexingOperationListener.IndexOperationDetails operationDetails = latestCopy.get(hit.getId());
            if (operationDetails == null) {
                Assert.fail("Difference between Index and Sink");
            }
            assertEquals(
                "source for " + operationDetails.docId() + " " + operationDetails.seqNo(),
                hit.getSourceAsMap(),
                SourceLookup.sourceAsMap(operationDetails.parsedDoc().source())
            );
        }
        Set<String> hits = Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
        for (String docId : docIds) {
            assertTrue("Missing expected doc: " + docId + " Actual: " + hits, hits.contains(docId));
        }
    }

    public void testConcurrentWritesWithPrimaryRelocation() throws Exception {
        createIndex("test", Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        internalCluster().startClusterManagerOnlyNode();
        ensureYellow("test");
        DiscoveryNode primaryNode = getNodeContainingPrimaryShard();
        String primary = primaryNode.getName();
        String replica = internalCluster().startDataOnlyNode();
        ensureGreen();
        final String newPrimary = internalCluster().startDataOnlyNode();
        int requestCount = randomIntBetween(2, 10);
        int batchSize = randomIntBetween(1, 10);
        final BulkResponse[] responses = new BulkResponse[requestCount];
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);
        Thread[] threads = new Thread[responses.length];
        FlushRequest request = new FlushRequest("test");
        request.waitIfOngoing(true);
        request.force(true);
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
                    requestBuilder.add(prepareDelete("val-" + (threadID - 1)));
                }
                BulkResponse bulkItemResponses = requestBuilder.get();
                responses[threadID] = bulkItemResponses;
                if (randomBoolean()) {
                    client().admin().indices().flush(request);
                }
            });
            threads[threadID].start();
        }

        // wait until sink has something in it from old primary
        assertBusy(() -> assertTrue(sink.counter > 0));
        logger.info("--> relocate the shard");
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, primary, newPrimary)).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertEquals(clusterHealthResponse.isTimedOut(), false);

        ensureYellowAndNoInitializingShards("test");

        // index 10 more after failover
        BulkRequestBuilder requestBuilder = client().prepareBulk();
        addDocs(requestBuilder, requestCount, batchSize);
        BulkResponse bulk = requestBuilder.get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertThat(refresh().getFailedShards(), equalTo(0));

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        client().admin().indices().prepareFlush("test").setForce(true).get();

        SearchResponse searchResponse = client().prepareSearch()
            .setSize(10000)
            .setTrackTotalHits(true)
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndices("test")
            .seqNoAndPrimaryTerm(true)
            .get();

        assertEquals(sink.getUniqueDocCount(), searchResponse.getHits().getTotalHits().value);
        Map<String, BatchIndexingOperationListener.IndexOperationDetails> latestCopy = sink.getLatestCopy();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BatchIndexingOperationListener.IndexOperationDetails operationDetails = latestCopy.get(hit.getId());
            if (operationDetails == null) {
                Assert.fail("Difference between Index and Sink");
            }
            assertEquals(
                "source for " + operationDetails.docId() + " " + operationDetails.seqNo(),
                hit.getSourceAsMap(),
                SourceLookup.sourceAsMap(operationDetails.parsedDoc().source())
            );
        }
    }

    protected DiscoveryNode getNodeContainingPrimaryShard() {
        final ClusterState state = getClusterState();
        final ShardRouting primaryShard = state.routingTable().index("test").shard(0).primaryShard();
        return state.nodes().resolveNode(primaryShard.currentNodeId());
    }

    private List<String> addDocs(BulkRequestBuilder requestBuilder, int offset, int count) {
        List<String> docIds = new ArrayList<>();
        for (int i = offset * count; i < (offset * count) + count; i++) {
            Map<String, Object> sourceAsMap = new HashMap<>();
            for (int j = 0; j < randomIntBetween(1, 5); j++) {
                sourceAsMap.put("field-" + j, UUID.randomUUID().toString());
            }
            String id = "val-" + i + "-" + offset;
            docIds.add(id);
            logger.info("making request for {} {}", id, sourceAsMap);
            requestBuilder.add(prepareIndex(id, sourceAsMap));
        }
        return docIds;
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

    static class TestSink implements BatchIndexingOperationListener.Sink, IndexEventListener {
        int counter = 0;

        public void clear() {
            this.ops.clear();
            this.docs.clear();
            this.counter = 0;
            this.seqNos.clear();
        }

        // counter of gen with list of ops
        public Map<Integer, SortedSet<BatchIndexingOperationListener.OperationDetails>> getOps() {
            return ops;
        }

        public int getUniqueDocCount() {
            return docs.size();
        }

        public Map<String, BatchIndexingOperationListener.IndexOperationDetails> getLatestCopy() {
            return latestCopy;
        }

        // docs per batch processed
        private Map<Integer, SortedSet<BatchIndexingOperationListener.OperationDetails>> ops = new HashMap<>();

        // all unique docs seen
        private Set<String> docs = new HashSet<>();

        // latest copy of each doc - can include deletes
        private Map<String, BatchIndexingOperationListener.IndexOperationDetails> latestCopy = new HashMap<>();

        // docs mapped to their seqno - to check for dupes with diff docId.
        private Map<Long, BatchIndexingOperationListener.OperationDetails> seqNos = new HashMap<>();

        @Override
        public long acceptBatch(ShardId shardId, SortedSet<BatchIndexingOperationListener.OperationDetails> operationDetails) {
            if (operationDetails.isEmpty()) {
                Assert.fail();
            }
            ops.put(counter, operationDetails);
            operationDetails.stream().forEach((op) -> {
                if (seqNos.containsKey(op.seqNo())) {
                    BatchIndexingOperationListener.OperationDetails existing = seqNos.get(op.seqNo());
                    if (existing.primaryTerm() == op.primaryTerm()) {
                        assertEquals(
                            "Operation sent to the sink with the same sequence number but different doc ID: " + op + " " + existing,
                            op.docId(),
                            existing.docId()
                        );
                    } else {
                        // this is ok across pterms, this means a req mid flight was rerouted to the new primary where the old had
                        // previously assigned a seqno to an op that hit the sink before the req was ack'd.
                    }
                }
                seqNos.put(op.seqNo(), op);
            });
            for (BatchIndexingOperationListener.OperationDetails operationDetail : operationDetails) {
                if (operationDetail instanceof BatchIndexingOperationListener.IndexOperationDetails) {
                    docs.add(operationDetail.docId());
                    latestCopy.put(operationDetail.docId(), (BatchIndexingOperationListener.IndexOperationDetails) operationDetail);
                } else {
                    docs.remove(operationDetail.docId());
                    latestCopy.put(operationDetail.docId(), null);
                }
            }
            counter++;
            return operationDetails.stream()
                .mapToLong(BatchIndexingOperationListener.OperationDetails::seqNo)
                .max()
                .orElse(SequenceNumbers.NO_OPS_PERFORMED);
        }
    }

    public void testSettingUpdates() throws ExecutionException, InterruptedException {
        createIndex("test");
        ensureGreen();
        DiscoveryNode primaryNode = getNodeContainingPrimaryShard();
        String primary = primaryNode.getName();
        IndexShard shard = getIndexShard(primary, "test");

        // defaults
        assertEquals(650, shard.getRemoteStoreSettings().getClusterBatchOperationListenerBufferInterval().millis());
        assertEquals(30000, shard.getRemoteStoreSettings().getClusterBatchOperationListenerPollTimeout().millis());
        assertEquals(60000, shard.getRemoteStoreSettings().getClusterBatchOperationListenerDrainTimeout().millis());

        // buffer interval
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder()
                .put(CLUSTER_BATCH_OPERATION_LISTENER_BUFFER_INTERVAL_SETTING.getKey(), "0ms")
                .put(CLUSTER_BATCH_OPERATION_LISTENER_POLL_TIMEOUT_SETTING.getKey(), "5s")
                .put(CLUSTER_BATCH_OPERATION_LISTENER_DRAIN_TIMEOUT_SETTING.getKey(), "30s")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        assertEquals(0, shard.getRemoteStoreSettings().getClusterBatchOperationListenerBufferInterval().millis());
        assertEquals(5000, shard.getRemoteStoreSettings().getClusterBatchOperationListenerPollTimeout().millis());
        assertEquals(30000, shard.getRemoteStoreSettings().getClusterBatchOperationListenerDrainTimeout().millis());
    }
}
