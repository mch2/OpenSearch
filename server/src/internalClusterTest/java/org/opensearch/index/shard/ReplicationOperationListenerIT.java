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
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

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
            .add(prepareIndex("multibulk1", "field1", "three")) // update field 1
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());

        assertThat(refresh().getFailedShards(), equalTo(0));

        assertEquals(1, sink.getOps().size());
        ReplicationSink.OperationDetails operationDetails = sink.getOps().get(sink.getOps().keySet().stream().findFirst().get()).stream().findFirst().get();
        assertEquals(ReplicationSink.IndexingOperationDetails.class, operationDetails.getClass());
        ReplicationSink.IndexingOperationDetails op = (ReplicationSink.IndexingOperationDetails) operationDetails;
        Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(op.parsedDoc().source());
        assertEquals(Map.of("field1", "three", "field2", "two"), sourceAsMap);
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

    public void testMultiThreaded() throws InterruptedException {
        createIndex("test");
        ensureGreen();
        final BulkResponse[] responses = new BulkResponse[30];
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(responses.length);
        Thread[] threads = new Thread[responses.length];

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
                    logger.info("prepareing {}", "val-" + threadID);
                    requestBuilder.add(
                        prepareIndex("val-" + threadID, "field1", "val-" + threadID)
                    );
                } catch (IOException e) {
                    Assert.fail();
                }
                responses[threadID] = requestBuilder.get();

            });
            threads[threadID].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        for (BulkResponse response : responses) {
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
                Assert.fail();
            }
        }
        int uniqueDocs = 0;
        Map<Integer, List<ReplicationSink.OperationDetails>> ops = sink.getOps();
        uniqueDocs = ops.values().stream().mapToInt(List::size).sum();
        assertEquals(uniqueDocs, 30);
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
        }

        public Map<Integer, List<OperationDetails>> getOps() {
            return ops;
        }

        private Map<Integer, List<ReplicationSink.OperationDetails>> ops = new HashMap<>();

        @Override
        public void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener) {
            if (operationDetails == null) {
                Assert.fail();
            }
            ops.put(counter, operationDetails);
            counter++;
            listener.onResponse(operationDetails.stream().mapToLong(OperationDetails::seqNo).max().getAsLong());
        }
    }
}
