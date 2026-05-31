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
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Hazard test for the unguarded {@code ArrowFlightProducer}: a slow consumer with a
 * tight flight-pool cap exhausts the allocator before all batches drain. Boots with
 * {@code arrow.flight.producer.backpressure.enabled=false}. The companion
 * {@link SlowConsumerWithBackpressureProducerIT} runs the same scenario with the
 * back-pressure producer enabled and asserts the stream completes cleanly instead.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2, maxNumDataNodes = 2)
public class SlowConsumerBackpressureIT extends OpenSearchIntegTestCase {

    static final long MB = 1024L * 1024;
    static final long FLIGHT_POOL_CAP_BYTES = 8 * MB;
    static final int ROWS_PER_BATCH = 256 * 1024;
    static final int BATCH_COUNT = 64;
    static final long CONSUMER_SLEEP_MS = 200;

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
        // Disable back-pressure to exercise the unguarded producer path.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.native_memory.limit", "256mb")
            .put(NativeAllocatorPoolConfig.SETTING_ROOT_LIMIT, 64 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_FLIGHT_MAX, FLIGHT_POOL_CAP_BYTES)
            .put(NativeAllocatorPoolConfig.SETTING_INGEST_MAX, 16 * MB)
            .put(NativeAllocatorPoolConfig.SETTING_QUERY_MAX, 16 * MB)
            .put(ServerConfig.FLIGHT_BACKPRESSURE_ENABLED.getKey(), false)
            .build();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSlowConsumerCausesProducerAllocatorOom() throws Exception {
        DiscoveryNode targetNode = pickRemoteDataNode();
        StreamTransportService sts = internalCluster().getInstance(StreamTransportService.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger batchesReceived = new AtomicInteger(0);
        AtomicBoolean completed = new AtomicBoolean(false);

        sts.sendRequest(
            targetNode,
            SlowConsumerAction.NAME,
            new SlowConsumerRequest(BATCH_COUNT, ROWS_PER_BATCH),
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
                            Thread.sleep(CONSUMER_SLEEP_MS);
                        }
                        completed.set(true);
                        stream.close();
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                        try {
                            stream.close();
                        } catch (Exception ignored) {
                            // already closing on error path
                        }
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

        assertTrue("Stream should finish (success or failure) within 60s", latch.await(60, TimeUnit.SECONDS));

        assertFalse("Stream completed normally; expected OOM under cap=" + FLIGHT_POOL_CAP_BYTES, completed.get());
        assertNotNull("Expected producer-side OOM to surface to client, got none", failure.get());

        String chain = describeCauseChain(failure.get());
        assertTrue(
            "Expected memory-exhaustion signal in cause chain, got: " + chain,
            chain.toLowerCase(Locale.ROOT).contains("memory") || chain.contains("OutOfMemoryException")
        );
        assertTrue("Consumer should have received at least one batch before OOM", batchesReceived.get() > 0);
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

    private static String describeCauseChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage()).append(" -> ");
            if (c.getCause() == c) break;
        }
        return sb.toString();
    }
}
