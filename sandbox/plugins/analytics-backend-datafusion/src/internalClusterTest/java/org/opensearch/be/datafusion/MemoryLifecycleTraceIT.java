/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.be.lucene.LucenePlugin;
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
import java.util.concurrent.ExecutionException;

/**
 * Integration test that traces Arrow memory allocator state at each step of a
 * coordinator-reduce query lifecycle. Not a pass/fail test — the value is the
 * logged trace showing exactly where each batch's memory lives, which allocator
 * owns it, and how bytes flow between pools.
 *
 * <p>Run with: {@code ./gradlew :sandbox:plugins:analytics-backend-datafusion:internalClusterTest
 *   --tests "*.MemoryLifecycleTraceIT" -Dtests.output=always}
 *
 * <p>The trace shows:
 * <ol>
 *   <li>Baseline: pool states before any query</li>
 *   <li>After index creation + data seeding</li>
 *   <li>During query execution (sampled)</li>
 *   <li>After query completes — verifying all pools return to baseline</li>
 *   <li>After concurrent queries — checking for accumulation</li>
 * </ol>
 *
 * <p>Each snapshot logs:
 * <ul>
 *   <li>POOL_FLIGHT: allocated / peak / limit (glibc, Java Unsafe — the transport buffers)</li>
 *   <li>POOL_QUERY: allocated / peak / limit (glibc, Java Unsafe — the query-side buffers)</li>
 *   <li>jemalloc allocated / resident (Rust/DataFusion hash tables, sort buffers)</li>
 *   <li>JVM heap used / max</li>
 *   <li>Process RSS</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class MemoryLifecycleTraceIT extends OpenSearchIntegTestCase {

    private static final Logger trace = LogManager.getLogger("MEMORY_TRACE");
    private static final String INDEX = "mem_trace_idx";
    private static final int NUM_SHARDS = 4;
    private static final int DOCS = 100_000;
    private static final int CARDINALITY = 10_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Snapshots all memory pools across all nodes and logs a trace line for each.
     */
    private void snapshot(String label) {
        trace.info("┌─── {} ───", label);
        NodesStatsResponse stats = client().admin().cluster()
            .nodesStats(new NodesStatsRequest().addMetric("jvm", "native_allocator", "native_memory"))
            .actionGet();
        for (NodeStats nodeStats : stats.getNodes()) {
            String nodeName = nodeStats.getNode().getName();
            // JVM heap
            long heapUsed = nodeStats.getJvm().getMem().getHeapUsed().getBytes();
            long heapMax = nodeStats.getJvm().getMem().getHeapMax().getBytes();

            trace.info("│ [{}] JVM heap: used={}MB max={}MB",
                nodeName, heapUsed / 1048576, heapMax / 1048576);

            // Native allocator pools (flight, query, ingest)
            if (nodeStats.getNativeAllocatorStats() != null) {
                nodeStats.getNativeAllocatorStats().getPools().forEach(pool -> {
                    trace.info("│ [{}] pool/{}: allocated={}B peak={}B limit={}B children={}",
                        nodeName, pool.getName(),
                        pool.getAllocatedBytes(), pool.getPeakBytes(),
                        pool.getLimitBytes(), pool.getChildCount());
                });
            }

            // Native memory (jemalloc — Rust/DataFusion)
            if (nodeStats.getAnalyticsBackendNativeMemoryStats() != null) {
                long jAlloc = nodeStats.getAnalyticsBackendNativeMemoryStats().getAllocatedBytes();
                long jResident = nodeStats.getAnalyticsBackendNativeMemoryStats().getResidentBytes();
                trace.info("│ [{}] jemalloc: allocated={}B ({}MB) resident={}B ({}MB) fragmentation={:.1f}x",
                    nodeName, jAlloc, jAlloc / 1048576, jResident, jResident / 1048576,
                    jAlloc > 0 ? (double) jResident / jAlloc : 0.0);
            }
        }
        trace.info("└─── end {} ───", label);
    }

    /**
     * Main test: traces the full lifecycle of a GROUP BY query across 2 nodes.
     * Read the log output to see exactly where memory is at each step.
     */
    public void testGroupByMemoryTrace() throws Exception {
        // ── Step 1: Baseline before any data ──
        snapshot("STEP 1: Baseline (empty cluster)");

        // ── Step 2: Create index and seed data ──
        createIndex();
        seedData();
        flush(INDEX);
        ensureGreen(INDEX);
        snapshot("STEP 2: After seeding " + DOCS + " docs");

        // ── Step 3: Run a single GROUP BY query and trace before/during/after ──
        trace.info("┌─── STEP 3: Single GROUP BY (cardinality={}) ───", CARDINALITY);
        snapshot("STEP 3a: Before query");

        PPLResponse response = runQuery("source=" + INDEX + " | stats count() by user_id | head 10");
        trace.info("│ Query returned {} rows", response.getResults().size());

        snapshot("STEP 3b: After query completes");

        // ── Step 4: Run 8 concurrent queries and check for accumulation ──
        trace.info("┌─── STEP 4: 8 concurrent GROUP BY queries ───");
        snapshot("STEP 4a: Before concurrent queries");

        var futures = new java.util.concurrent.CompletableFuture[8];
        for (int i = 0; i < 8; i++) {
            final int idx = i;
            futures[i] = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    PPLResponse r = runQuery("source=" + INDEX + " | stats count() by user_id | head 5");
                    trace.info("│ Concurrent query {} returned {} rows", idx, r.getResults().size());
                } catch (Exception e) {
                    trace.info("│ Concurrent query {} FAILED: {}", idx, e.getMessage());
                }
            });
        }
        java.util.concurrent.CompletableFuture.allOf(futures).join();

        snapshot("STEP 4b: After all concurrent queries complete");

        // ── Step 5: Wait for GC/cleanup and verify pools return to baseline ──
        Thread.sleep(2000);
        snapshot("STEP 5: After 2s settle (pools should be near-zero)");

        // ── Step 6: Run a high-cardinality query (stresses hash table + output) ──
        trace.info("┌─── STEP 6: Full cardinality GROUP BY (all {} groups) ───", CARDINALITY);
        snapshot("STEP 6a: Before high-cardinality query");

        try {
            PPLResponse bigResponse = runQuery("source=" + INDEX + " | stats count(), sum(amount) by user_id, phrase");
            trace.info("│ High-cardinality query returned {} rows", bigResponse.getResults().size());
        } catch (Exception e) {
            trace.info("│ High-cardinality query FAILED (expected if memory-bounded): {}", e.getMessage());
        }

        snapshot("STEP 6b: After high-cardinality query");
        Thread.sleep(2000);
        snapshot("STEP 6c: After 2s settle");
    }

    // ── Helpers ──

    private void createIndex() {
        CreateIndexResponse resp = client().admin().indices().prepareCreate(INDEX)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
            )
            .setMapping(
                "user_id", "type=long",
                "phrase", "type=keyword",
                "amount", "type=float",
                "category", "type=keyword"
            )
            .get();
        assertTrue(resp.isAcknowledged());
    }

    private void seedData() {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulkRequest.add(client().prepareIndex(INDEX).setSource(
                "user_id", i % CARDINALITY,
                "phrase", "phrase_" + (i % (CARDINALITY / 2)),
                "amount", Math.random() * 10000,
                "category", "cat_" + (i % 1000)
            ));
            if (bulkRequest.numberOfActions() >= 5000) {
                bulkRequest.get();
                bulkRequest = client().prepareBulk();
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkRequest.get();
        }
    }

    private PPLResponse runQuery(String ppl) throws ExecutionException, InterruptedException {
        return client().execute(
            UnifiedPPLExecuteAction.INSTANCE,
            new PPLRequest(ppl, null, null)
        ).get();
    }
}
