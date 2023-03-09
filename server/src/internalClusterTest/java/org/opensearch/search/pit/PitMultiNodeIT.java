/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.action.search.PitTestsUtil;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Requests;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.SegmentReplicationBaseIT;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.action.search.PitTestsUtil.assertSegments;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;

/**
 * Multi node integration tests for PIT creation and search operation with PIT ID.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class PitMultiNodeIT extends SegmentReplicationBaseIT {

    @Before
    public void setupIndex() throws ExecutionException, InterruptedException {
//        createIndex("index", Settings.builder().put(super.indexSettings()).build());
//        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
//        ensureGreen();
    }

    @After
    public void clearIndex() {
        client().admin().indices().prepareDelete(INDEX_NAME).get();
    }

    public void testScroll() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 100; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            refresh(INDEX_NAME);
        }
        assertBusy(() -> {
            assertEquals(getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(), getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion());
        });
        final IndexShard primaryShard = getIndexShard(primary, INDEX_NAME);
        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);
        SearchResponse searchResponse = client(replica).prepareSearch()
            .setQuery(matchAllQuery())
            .setIndices(INDEX_NAME)
            .setRequestCache(false)
            .setPreference("_only_local")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .addSort("field", SortOrder.ASC)
            .setSize(10)
            .setScroll(TimeValue.timeValueDays(1))
            .get();
        logger.info("Hits : {}", searchResponse.getHits().getTotalHits());

        flush(INDEX_NAME);

        for (int i = 3; i < 50; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            refresh(INDEX_NAME);
            if (randomBoolean()) {
                client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
                flush(INDEX_NAME);
            }
        }
        assertBusy(() -> {
            assertEquals(getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(), getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion());
        });

        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertBusy(() -> {
            assertEquals(getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(), getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion());
        });
        // Test stats
        long numHits = 0;
        do {
            numHits += searchResponse.getHits().getHits().length;
            searchResponse = client(replica)
                .prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueDays(1)).get();
            assertAllSuccessful(searchResponse);
            logger.info("Num hits {}", numHits);
        } while (searchResponse.getHits().getHits().length > 0);
    }

    private Releasable blockReplication(List<String> nodes, CountDownLatch latch) {
        CountDownLatch pauseReplicationLatch = new CountDownLatch(nodes.size());
        for (String node : nodes) {

            MockTransportService mockTargetTransportService = ((MockTransportService) internalCluster().getInstance(
                TransportService.class,
                node
            ));
            mockTargetTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES)) {
                    try {
                        latch.countDown();
                        pauseReplicationLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        return () -> {
            while (pauseReplicationLatch.getCount() > 0) {
                pauseReplicationLatch.countDown();
            }
        };
    }

    public void testPit() throws Exception {
        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("1")
            .setSource("foo", randomInt()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo", randomInt()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        for (int i = 3; i < 100; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", randomInt()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            refresh(INDEX_NAME);
        }
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), false);
        request.setPreference("_only_local");
        request.setIndices(new String[] { INDEX_NAME });
        ActionFuture<CreatePitResponse> execute = client(replica)
            .execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        SearchResponse searchResponse = client(replica).prepareSearch(INDEX_NAME)
            .setSize(10)
            .setPreference("_only_local")
            .setRequestCache(false)
            .addSort("foo", SortOrder.ASC)
            .searchAfter(new Object[] { 30 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        logger.info("Hits : {}", List.of(searchResponse.getHits().getHits()));
        assertEquals(1, searchResponse.getSuccessfulShards());
        assertEquals(1, searchResponse.getTotalShards());
        FlushRequest flushRequest = Requests.flushRequest(INDEX_NAME);
        client().admin().indices().flush(flushRequest).get();
        final IndexShard primaryShard = getIndexShard(primary, INDEX_NAME);
        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);

        flush(INDEX_NAME);
        logger.info("Primary store {}", List.of(primaryShard.store().directory().listAll()));
        logger.info("Replica store {}", List.of(replicaShard.store().directory().listAll()));

        for (int i = 101; i < 200; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", randomInt()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            refresh(INDEX_NAME);
            if (randomBoolean()) {
                client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
                flush(INDEX_NAME);
            }
        }
        logger.info("Primary store {}", List.of(primaryShard.store().directory().listAll()));
        logger.info("Replica store {}", List.of(replicaShard.store().directory().listAll()));
        assertBusy(() -> {
            assertEquals(getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(), getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion());
        });

        logger.info("Before force merge");
        logger.info("Primary store {}", List.of(primaryShard.store().directory().listAll()));
        logger.info("Replica store {}", List.of(replicaShard.store().directory().listAll()));

        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertBusy(() -> {
            assertEquals(getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(), getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion());
        });
        // Test stats
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(INDEX_NAME);
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().stats(indicesStatsRequest).get();
        long pitCurrent = indicesStatsResponse.getIndex(INDEX_NAME).getTotal().search.getTotal().getPitCurrent();
        long openContexts = indicesStatsResponse.getIndex(INDEX_NAME).getTotal().search.getOpenContexts();
        assertEquals(1, pitCurrent);
        assertEquals(1, openContexts);
        logger.info("Primary store {}", List.of(primaryShard.store().directory().listAll()));
        logger.info("Replica store {}", List.of(replicaShard.store().directory().listAll()));
        SearchResponse resp = client(replica).prepareSearch(INDEX_NAME)
            .setSize(10)
            .setPreference("_only_local")
            .addSort("foo", SortOrder.ASC)
            .searchAfter(new Object[] { 30 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .setRequestCache(false)
            .get();
        logger.info("Hits : {}", List.of(resp.getHits().getHits()));
        PitTestsUtil.assertUsingGetAllPits(client(replica), pitResponse.getId(), pitResponse.getCreationTime());
        assertSegments(false, client(replica), pitResponse.getId());
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationFalse() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), false);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Failed to execute phase [create_pit]"));
                validatePitStats("index", 0, 0);
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationTrue() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
                CreatePitResponse pitResponse = execute.get();
                PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime());
                assertSegments(false, "index", 1, client(), pitResponse.getId());
                assertEquals(1, pitResponse.getSuccessfulShards());
                assertEquals(2, pitResponse.getTotalShards());
                SearchResponse searchResponse = client().prepareSearch("index")
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getTotalShards());
                validatePitStats("index", 1, 1);
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDrop() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                SearchResponse searchResponse = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getFailedShards());
                assertEquals(0, searchResponse.getSkippedShards());
                assertEquals(2, searchResponse.getTotalShards());
                validatePitStats("index", 1, 1);
                PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime());
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDropWithPartialSearchResultsFalse() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<SearchResponse> execute = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .setAllowPartialSearchResults(false)
                    .execute();
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Partial shards failure"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitInvalidDefaultKeepAlive() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "1m").put("search.default_keep_alive", "2m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (2m > 1m)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "5m").put("point_in_time.max_keep_alive", "5m"))
                .get()
        );
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "2m"))
                .get()
        );
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "2m"))
                .get()
        );
        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "3m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (3m > 2m)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "1m"))
                .get()
        );
        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("point_in_time.max_keep_alive", "30s"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (1m > 30s)"));
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testConcurrentCreates() throws InterruptedException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });

        int concurrentRuns = randomIntBetween(20, 50);
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            Set<String> createSet = new HashSet<>();
            for (int i = 0; i < concurrentRuns; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering pit create --->");
                    LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                        @Override
                        public void onResponse(CreatePitResponse createPitResponse) {
                            if (createSet.add(createPitResponse.getId())) {
                                numSuccess.incrementAndGet();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {}
                    }, countDownLatch);
                    client().execute(CreatePitAction.INSTANCE, createPitRequest, listener);
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testConcurrentCreatesWithDeletes() throws InterruptedException, ExecutionException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });
        List<String> pitIds = new ArrayList<>();
        String id = client().execute(CreatePitAction.INSTANCE, createPitRequest).get().getId();
        pitIds.add(id);
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitIds);
        Set<String> createSet = new HashSet<>();
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            int concurrentRuns = randomIntBetween(20, 50);

            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            long randomDeleteThread = randomLongBetween(0, concurrentRuns - 1);
            for (int i = 0; i < concurrentRuns; i++) {
                int currentThreadIteration = i;
                Runnable thread = () -> {
                    if (currentThreadIteration == randomDeleteThread) {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<CreatePitResponse>() {
                            @Override
                            public void onResponse(CreatePitResponse createPitResponse) {
                                if (createSet.add(createPitResponse.getId())) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(CreatePitAction.INSTANCE, createPitRequest, listener);
                    } else {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<DeletePitResponse>() {
                            @Override
                            public void onResponse(DeletePitResponse deletePitResponse) {
                                if (deletePitResponse.getDeletePitResults().get(0).isSuccessful()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(DeletePitAction.INSTANCE, deletePITRequest, listener);
                    }
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void validatePitStats(String index, long expectedPitCurrent, long expectedOpenContexts) throws ExecutionException,
        InterruptedException {
        // Clear the index transaction log
        FlushRequest flushRequest = Requests.flushRequest(index);
        client().admin().indices().flush(flushRequest).get();
        // Test stats
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(index);
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().stats(indicesStatsRequest).get();
        long pitCurrent = indicesStatsResponse.getIndex(index).getTotal().search.getTotal().getPitCurrent();
        long openContexts = indicesStatsResponse.getIndex(index).getTotal().search.getOpenContexts();
        assertEquals(expectedPitCurrent, pitCurrent);
        assertEquals(expectedOpenContexts, openContexts);
    }

    public void testGetAllPits() throws Exception {
        client().admin().indices().prepareCreate("index1").get();
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        CreatePitResponse pitResponse1 = client().execute(CreatePitAction.INSTANCE, request).get();
        CreatePitResponse pitResponse2 = client().execute(CreatePitAction.INSTANCE, request).get();
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        ActionFuture<GetAllPitNodesResponse> execute1 = client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
        GetAllPitNodesResponse getPitResponse = execute1.get();
        assertEquals(3, getPitResponse.getPitInfos().size());
        List<String> resultPitIds = getPitResponse.getPitInfos().stream().map(p -> p.getPitId()).collect(Collectors.toList());
        // asserting that we get all unique PIT IDs
        Assert.assertTrue(resultPitIds.contains(pitResponse.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse1.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse2.getId()));
        client().admin().indices().prepareDelete("index1").get();
    }

    public void testGetAllPitsDuringNodeDrop() throws Exception {
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(getDiscoveryNodes());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<GetAllPitNodesResponse> execute1 = client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
                GetAllPitNodesResponse getPitResponse = execute1.get();
                // we still get a pit id from the data node which is up
                assertEquals(1, getPitResponse.getPitInfos().size());
                // failure for node drop
                assertEquals(1, getPitResponse.failures().size());
                assertTrue(getPitResponse.failures().get(0).getMessage().contains("Failed node"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    private DiscoveryNode[] getDiscoveryNodes() throws ExecutionException, InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        return disNodesArr;
    }

    public void testConcurrentGetWithDeletes() throws InterruptedException, ExecutionException {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "index" });
        List<String> pitIds = new ArrayList<>();
        String id = client().execute(CreatePitAction.INSTANCE, createPitRequest).get().getId();
        pitIds.add(id);
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitIds);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(getDiscoveryNodes());
        AtomicInteger numSuccess = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(PitMultiNodeIT.class.getName());
            int concurrentRuns = randomIntBetween(20, 50);

            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            long randomDeleteThread = randomLongBetween(0, concurrentRuns - 1);
            for (int i = 0; i < concurrentRuns; i++) {
                int currentThreadIteration = i;
                Runnable thread = () -> {
                    if (currentThreadIteration == randomDeleteThread) {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<GetAllPitNodesResponse>() {
                            @Override
                            public void onResponse(GetAllPitNodesResponse getAllPitNodesResponse) {
                                if (getAllPitNodesResponse.failures().isEmpty()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest, listener);
                    } else {
                        LatchedActionListener listener = new LatchedActionListener<>(new ActionListener<DeletePitResponse>() {
                            @Override
                            public void onResponse(DeletePitResponse deletePitResponse) {
                                if (deletePitResponse.getDeletePitResults().get(0).isSuccessful()) {
                                    numSuccess.incrementAndGet();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {}
                        }, countDownLatch);
                        client().execute(DeletePitAction.INSTANCE, deletePITRequest, listener);
                    }
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertEquals(concurrentRuns, numSuccess.get());

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

}
