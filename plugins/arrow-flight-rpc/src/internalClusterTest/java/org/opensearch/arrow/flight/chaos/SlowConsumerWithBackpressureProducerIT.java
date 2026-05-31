/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.chaos;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.arrow.flight.chaos.SlowConsumerSupport.SlowConsumerAction;
import org.opensearch.arrow.flight.chaos.SlowConsumerSupport.SlowConsumerRequest;
import org.opensearch.arrow.flight.chaos.SlowConsumerSupport.SlowConsumerResponse;
import org.opensearch.arrow.flight.chaos.SlowConsumerSupport.SlowConsumerTestPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Same slow-consumer scenario as {@link SlowConsumerBackpressureIT} but with the
 * back-pressure producer enabled. The stream must complete cleanly: the producer
 * thread parks in {@code awaitReadyOrThrow} once gRPC's outbound buffer is full
 * instead of OOMing the flight-pool allocator.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2, maxNumDataNodes = 2)
public class SlowConsumerWithBackpressureProducerIT extends OpenSearchIntegTestCase {

    private static final long MB = 1024L * 1024;

    /** Pool sized to absorb gRPC threshold + eventloop queue overshoot before back-pressure
     *  engages. The eventloop's queue is unbounded, so a tight-loop producer can still
     *  overshoot before {@code isReady()} flips false; that case is the companion hazard test. */
    private static final long FLIGHT_POOL_CAP_BYTES = 128 * MB;

    private static final int ROWS_PER_BATCH = SlowConsumerBackpressureIT.ROWS_PER_BATCH;
    private static final int BATCH_COUNT = SlowConsumerBackpressureIT.BATCH_COUNT;
    private static final long CONSUMER_SLEEP_MS = SlowConsumerBackpressureIT.CONSUMER_SLEEP_MS;

    /** Per-batch producer compute. Producer remains faster than consumer so back-pressure
     *  must engage, but slow enough that the eventloop queue can't grow unboundedly. */
    private static final long PRODUCER_SLEEP_MS = 30;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SlowConsumerTestPlugin.class, ArrowBasePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                FlightStreamPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                FlightStreamPlugin.class.getName(),
                null,
                List.of(ArrowBasePlugin.class.getName()),
                false
            )
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.native_memory.limit", "512mb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 256 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, FLIGHT_POOL_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 16 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 16 * MB)
            .put(ServerConfig.FLIGHT_BACKPRESSURE_ENABLED.getKey(), true)
            .put(ServerConfig.FLIGHT_READY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(60))
            .build();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSlowConsumerCompletesUnderBackpressureProducer() throws Exception {
        DiscoveryNode targetNode = pickRemoteDataNode();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger batchesReceived = new AtomicInteger(0);

        long startNanos = System.nanoTime();
        sts.sendRequest(
            targetNode,
            SlowConsumerAction.NAME,
            new SlowConsumerRequest(BATCH_COUNT, ROWS_PER_BATCH, PRODUCER_SLEEP_MS),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new ArrowBatchResponseHandler<SlowConsumerResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<SlowConsumerResponse> stream) {
                    try {
                        SlowConsumerResponse response;
                        while ((response = stream.nextResponse()) != null) {
                            try (VectorSchemaRoot root = response.getRoot()) {
                                batchesReceived.incrementAndGet();
                            }
                            // Slow consumer drives gRPC's outbound past the readiness
                            // threshold so the producer must park rather than OOM the pool.
                            Thread.sleep(CONSUMER_SLEEP_MS);
                        }
                        stream.close();
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                        stream.cancel("consumer error", e);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    failure.compareAndSet(null, exp);
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }

                @Override
                public SlowConsumerResponse read(StreamInput in) throws IOException {
                    return new SlowConsumerResponse(in);
                }
            }
        );

        assertTrue("Stream should finish within 90s", latch.await(90, TimeUnit.SECONDS));
        assertNull("Back-pressure producer must not surface any failure: " + failure.get(), failure.get());

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        assertEquals("All batches must arrive successfully under back-pressure", BATCH_COUNT, batchesReceived.get());

        // Wall-clock must reflect consumer pacing — without back-pressure the producer
        // would race ahead and finish near-instantly relative to the consumer's sleep
        // budget. Use 0.5x as the floor to absorb startup jitter without flakiness.
        long minExpectedMillis = (long) ((BATCH_COUNT * CONSUMER_SLEEP_MS) * 0.5);
        assertTrue(
            "Wall-clock " + elapsedMillis + "ms must reflect consumer pacing (>=" + minExpectedMillis + "ms)",
            elapsedMillis >= minExpectedMillis
        );
    }

    private DiscoveryNode pickRemoteDataNode() {
        String localName = internalCluster().getInstance(StreamTransportService.class).getLocalNode().getName();
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (!node.getName().equals(localName) && node.isDataNode()) {
                return node;
            }
        }
        throw new AssertionError("No remote data node found");
    }
}
