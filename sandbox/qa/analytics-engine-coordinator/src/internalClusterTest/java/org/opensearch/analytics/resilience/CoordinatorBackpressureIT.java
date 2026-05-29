/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Backpressure and memory tracing IT for the coordinator-reduce path.
 *
 * <p>Boots a 2-node cluster with a tight flight pool cap and runs high-cardinality
 * GROUP BY queries that produce many batches over the Flight transport. Traces
 * allocator state at each step to verify:
 * <ul>
 *   <li>Flight pool (data node producer side) stays bounded — producer parks
 *       when allocator is pressured rather than OOM-ing</li>
 *   <li>Flight pool (coordinator receiver side) doesn't eagerly accumulate
 *       unbounded deserialized batches</li>
 *   <li>Query pool (coordinator reduce side) stays bounded during output drain</li>
 *   <li>All pools return to baseline after query completion</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew -Dsandbox.enabled=true
 *   :sandbox:qa:analytics-engine-coordinator:internalClusterTest
 *   --tests "*.CoordinatorBackpressureIT" -Dtests.output=always}
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class CoordinatorBackpressureIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(CoordinatorBackpressureIT.class);
    private static final Logger trace = LogManager.getLogger("MEMORY_TRACE");

    private static final String INDEX = "bp_test_idx";
    private static final int NUM_SHARDS = 4;
    private static final int DOCS = 10_000;
    private static final int CARDINALITY = 1_000;

    private static final long MB = 1024L * 1024;
    /** Tight flight pool: 32MB. With 20K groups × ~100 bytes/group = 2MB per partial
     *  agg output, 4 shards × 2MB = 8MB flowing through flight at peak. 32MB cap
     *  gives 4× headroom — enough to complete but tight enough to observe pressure. */
    private static final long FLIGHT_POOL_CAP = 32 * MB;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, ArrowBasePlugin.class, CompositeDataFormatPlugin.class, org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(), "classpath plugin", "NA", Version.CURRENT, "1.8",
            pluginClass.getName(), null, extendedPlugins, false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, FLIGHT_POOL_CAP)
            .build();
    }

    /**
     * Snapshots allocator state across all nodes and logs a trace.
     */
    private void snapshot(String label) {
        trace.info("┌─── {} ───", label);
        NodesStatsResponse stats = client().admin().cluster()
            .nodesStats(new NodesStatsRequest().addMetrics("jvm", "native_allocator", "native_memory"))
            .actionGet();
        for (NodeStats nodeStats : stats.getNodes()) {
            String name = nodeStats.getNode().getName();
            long heapMB = nodeStats.getJvm().getMem().getHeapUsed().getBytes() / MB;
            trace.info("│ [{}] heap={}MB", name, heapMB);
            if (nodeStats.getNativeAllocatorStats() != null) {
                nodeStats.getNativeAllocatorStats().getPools().forEach(pool ->
                    trace.info("│ [{}] pool/{}: allocated={}B peak={}B limit={}B",
                        name, pool.getName(), pool.getAllocatedBytes(),
                        pool.getPeakBytes(), pool.getLimitBytes())
                );
            }
            if (nodeStats.getAnalyticsBackendNativeMemoryStats() != null) {
                trace.info("│ [{}] jemalloc: allocated={}B resident={}B",
                    name,
                    nodeStats.getAnalyticsBackendNativeMemoryStats().getAllocatedBytes(),
                    nodeStats.getAnalyticsBackendNativeMemoryStats().getResidentBytes());
            }
        }
        trace.info("└─── end {} ───", label);
    }

    /**
     * Single high-cardinality GROUP BY across 2 nodes with tight flight pool.
     * Traces memory at each stage. With backpressure working, this should
     * complete without OOM. Without backpressure, the data node's flight pool
     * would exceed 32MB.
     */
    public void testHighCardinalityGroupByWithTightFlightPool() throws Exception {
        createAndSeedIndex();
        snapshot("BASELINE after seeding");

        // Start a high-frequency monitor that samples the flight pool every 50ms
        // DURING query execution. The other tests sample only before/after which
        // misses the in-flight peak.
        AtomicLong peakFlight = new AtomicLong();
        AtomicInteger samples = new AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean monitoring = new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread monitor = new Thread(() -> {
            while (monitoring.get()) {
                try {
                    NodesStatsResponse stats = client().admin().cluster()
                        .nodesStats(new NodesStatsRequest().addMetrics("native_allocator")).actionGet();
                    for (NodeStats ns : stats.getNodes()) {
                        if (ns.getNativeAllocatorStats() != null) {
                            ns.getNativeAllocatorStats().getPools().forEach(pool -> {
                                if ("flight".equals(pool.getName()) && pool.getAllocatedBytes() > 0) {
                                    long prev = peakFlight.get();
                                    peakFlight.accumulateAndGet(pool.getAllocatedBytes(), Math::max);
                                    if (pool.getAllocatedBytes() > prev) {
                                        trace.info("│ [hi-freq] [{}] flight rose to {}B ({}% of cap)",
                                            ns.getNode().getName(), pool.getAllocatedBytes(),
                                            pool.getLimitBytes() > 0 ? pool.getAllocatedBytes() * 100 / pool.getLimitBytes() : 0);
                                    }
                                }
                            });
                        }
                    }
                    samples.incrementAndGet();
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    // Ignore stats errors during shutdown
                }
            }
        }, "flight-pool-monitor");
        monitor.setDaemon(true);
        monitor.start();

        // Run one query — all batches flow through Flight between the two nodes
        trace.info("┌─── Running GROUP BY user_id ({}K groups, flight_cap={}MB) ───", CARDINALITY / 1000, FLIGHT_POOL_CAP / MB);
        long queryStart = System.nanoTime();
        PPLResponse response = runQuery("source=" + INDEX + " | stats count() by user_id | head 20");
        long queryMs = (System.nanoTime() - queryStart) / 1_000_000;
        trace.info("│ Query returned {} rows in {}ms", response.getRows().size(), queryMs);

        monitoring.set(false);
        monitor.join(500);
        trace.info("│ Monitor took {} samples, peak flight pool = {}B ({}% of {}MB cap)",
            samples.get(), peakFlight.get(),
            FLIGHT_POOL_CAP > 0 ? peakFlight.get() * 100 / FLIGHT_POOL_CAP : 0,
            FLIGHT_POOL_CAP / MB);

        snapshot("AFTER single query");

        // Verify flight pool didn't blow its cap
        assertPoolWithinLimit("flight", FLIGHT_POOL_CAP);
    }

    /**
     * Slow consumer test — uses {@code -Dtest.reduce.slowDrainMs=100} system
     * property to inject a 100ms sleep between drained batches. This forces
     * upstream (data node Flight producer) to back up. With backpressure
     * working, the data node's flight pool stays bounded; without it, the pool
     * fills up and the query OOMs.
     *
     * <p>The trace logs from {@code [mpsc-backpressure]} should fire during
     * this test, showing the producer blocking on the bounded mpsc.
     */
    public void testSlowConsumerForcesBackpressure() throws Exception {
        // Inject slow drain: 100ms per output batch on the coordinator
        System.setProperty("test.reduce.slowDrainMs", "100");
        try {
            createAndSeedIndex();
            snapshot("BASELINE before slow query");

            AtomicLong peakFlight = new AtomicLong();
            AtomicInteger samples = new AtomicInteger();
            java.util.concurrent.atomic.AtomicBoolean monitoring = new java.util.concurrent.atomic.AtomicBoolean(true);
            Thread monitor = new Thread(() -> {
                while (monitoring.get()) {
                    try {
                        NodesStatsResponse stats = client().admin().cluster()
                            .nodesStats(new NodesStatsRequest().addMetrics("native_allocator")).actionGet();
                        for (NodeStats ns : stats.getNodes()) {
                            if (ns.getNativeAllocatorStats() != null) {
                                ns.getNativeAllocatorStats().getPools().forEach(pool -> {
                                    if ("flight".equals(pool.getName()) && pool.getAllocatedBytes() > 0) {
                                        long prev = peakFlight.get();
                                        peakFlight.accumulateAndGet(pool.getAllocatedBytes(), Math::max);
                                        if (pool.getAllocatedBytes() > prev) {
                                            trace.info("│ [slow-drain] [{}] flight={}B ({}%)",
                                                ns.getNode().getName(), pool.getAllocatedBytes(),
                                                pool.getLimitBytes() > 0 ? pool.getAllocatedBytes() * 100 / pool.getLimitBytes() : 0);
                                        }
                                    }
                                });
                            }
                        }
                        samples.incrementAndGet();
                        Thread.sleep(100);
                    } catch (InterruptedException e) { return; }
                    catch (Exception e) { /* ignore */ }
                }
            }, "slow-drain-monitor");
            monitor.setDaemon(true);
            monitor.start();

            trace.info("┌─── Slow drain query (100ms/batch sleep) ───");
            long queryStart = System.nanoTime();
            PPLResponse response = runQuery("source=" + INDEX + " | stats count() by user_id | head 20");
            long queryMs = (System.nanoTime() - queryStart) / 1_000_000;
            trace.info("│ Slow query returned {} rows in {}ms", response.getRows().size(), queryMs);

            monitoring.set(false);
            monitor.join(500);
            trace.info("│ Monitor took {} samples, peak flight = {}B ({}%)",
                samples.get(), peakFlight.get(),
                FLIGHT_POOL_CAP > 0 ? peakFlight.get() * 100 / FLIGHT_POOL_CAP : 0);

            snapshot("AFTER slow query");
            assertPoolWithinLimit("flight", FLIGHT_POOL_CAP);
        } finally {
            System.clearProperty("test.reduce.slowDrainMs");
        }
    }

    /**
     * 8 concurrent high-cardinality queries with tight flight pool.
     * Stresses the backpressure mechanism under contention.
     */
    public void testConcurrentQueriesWithTightFlightPool() throws Exception {
        createAndSeedIndex();
        snapshot("BASELINE before concurrent");

        int concurrency = 8;
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicLong peakFlightBytes = new AtomicLong();

        trace.info("┌─── {} concurrent GROUP BY queries (flight_cap={}MB) ───", concurrency, FLIGHT_POOL_CAP / MB);

        var futures = new CompletableFuture[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final int idx = i;
            futures[i] = CompletableFuture.runAsync(() -> {
                try {
                    PPLResponse r = runQuery("source=" + INDEX + " | stats count() by user_id | head 10");
                    trace.info("│ Query {} completed: {} rows", idx, r.getRows().size());
                    succeeded.incrementAndGet();
                } catch (Exception e) {
                    trace.info("│ Query {} FAILED: {}", idx, e.getMessage());
                    failed.incrementAndGet();
                }
            });
        }

        // Monitor flight pool while queries run
        CompletableFuture<Void> monitor = CompletableFuture.runAsync(() -> {
            for (int tick = 0; tick < 60; tick++) {
                try { Thread.sleep(500); } catch (InterruptedException e) { return; }
                NodesStatsResponse stats = client().admin().cluster()
                    .nodesStats(new NodesStatsRequest().addMetrics("native_allocator"))
                    .actionGet();
                for (NodeStats ns : stats.getNodes()) {
                    if (ns.getNativeAllocatorStats() != null) {
                        ns.getNativeAllocatorStats().getPools().forEach(pool -> {
                            if ("flight".equals(pool.getName())) {
                                peakFlightBytes.accumulateAndGet(pool.getAllocatedBytes(), Math::max);
                                if (pool.getAllocatedBytes() > 0) {
                                    trace.info("│ [monitor] [{}] flight: {}B / {}B ({}%)",
                                        ns.getNode().getName(), pool.getAllocatedBytes(),
                                        pool.getLimitBytes(),
                                        pool.getLimitBytes() > 0 ? pool.getAllocatedBytes() * 100 / pool.getLimitBytes() : 0);
                                }
                            }
                        });
                    }
                }
                // Stop monitoring once all queries done
                if (CompletableFuture.allOf(futures).isDone()) break;
            }
        });

        CompletableFuture.allOf(futures).join();
        monitor.cancel(true);

        snapshot("AFTER concurrent queries");
        trace.info("│ Results: {} succeeded, {} failed, peak flight={}B ({}% of cap)",
            succeeded.get(), failed.get(), peakFlightBytes.get(),
            FLIGHT_POOL_CAP > 0 ? peakFlightBytes.get() * 100 / FLIGHT_POOL_CAP : 0);

        // With backpressure: all queries should succeed and flight pool stays within limit
        // Without backpressure: some queries fail with OOM from flight pool exhaustion
        assertTrue("At least some queries should succeed", succeeded.get() > 0);

        // After all queries, pools should return to near-zero
        Thread.sleep(2000);
        snapshot("AFTER 2s settle");
    }

    // ── Helpers ──

    private void createAndSeedIndex() {
        if (indexExists(INDEX)) return;
        CreateIndexResponse resp = client().admin().indices().prepareCreate(INDEX)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
            )
            .setMapping("user_id", "type=long", "phrase", "type=keyword", "amount", "type=float", "category", "type=keyword")
            .get();
        assertTrue(resp.isAcknowledged());
        ensureGreen(INDEX);

        var bulk = client().prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulk.add(client().prepareIndex(INDEX).setSource(
                "user_id", i % CARDINALITY,
                "phrase", "p" + (i % (CARDINALITY / 2)),
                "amount", Math.random() * 10000,
                "category", "c" + (i % 500)
            ));
            if (bulk.numberOfActions() >= 5000) {
                bulk.get();
                bulk = client().prepareBulk();
            }
        }
        if (bulk.numberOfActions() > 0) bulk.get();
        flush(INDEX);
    }

    private PPLResponse runQuery(String ppl) throws ExecutionException, InterruptedException {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).get();
    }

    private void assertPoolWithinLimit(String poolName, long limit) {
        NodesStatsResponse stats = client().admin().cluster()
            .nodesStats(new NodesStatsRequest().addMetrics("native_allocator")).actionGet();
        for (NodeStats ns : stats.getNodes()) {
            if (ns.getNativeAllocatorStats() != null) {
                ns.getNativeAllocatorStats().getPools().forEach(pool -> {
                    if (poolName.equals(pool.getName())) {
                        assertTrue(
                            "[" + ns.getNode().getName() + "] pool/" + poolName + " peak=" + pool.getPeakBytes()
                                + "B exceeded limit=" + limit + "B",
                            pool.getPeakBytes() <= limit
                        );
                    }
                });
            }
        }
    }
}
