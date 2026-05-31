/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.flight.stats.FlightCallTracker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the producer chosen at startup determines which {@link FlightServerChannel}
 * subclass is created per stream. Uses {@link FlightTransportTestBase} to obtain a real
 * {@link FlightTransport} (its {@code getRequestHandlers} is final and not Mockito-stubbable).
 */
public class BackpressureArrowFlightProducerTests extends FlightTransportTestBase {

    @SuppressWarnings("unchecked")
    private final FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey = mock(FlightServerMiddleware.Key.class);

    /**
     * The back-pressure producer must instantiate a {@link BackpressureFlightServerChannel}.
     */
    public void testCreateChannelReturnsBackpressureChannel() {
        BufferAllocator allocator = mock(BufferAllocator.class);
        BackpressureArrowFlightProducer producer = new BackpressureArrowFlightProducer(
            flightTransport,
            allocator,
            middlewareKey,
            statsCollector,
            5_000L
        );

        FlightServerChannel channel = producer.createChannel(newListener(), newMiddleware(), mock(FlightCallTracker.class));
        try {
            assertTrue(
                "Expected BackpressureFlightServerChannel, got " + channel.getClass().getName(),
                channel instanceof BackpressureFlightServerChannel
            );
        } finally {
            channel.close();
        }
    }

    /**
     * The default (non-backpressure) producer must continue to instantiate the base
     * {@link FlightServerChannel} — regression check on the parent's {@code createChannel}.
     */
    public void testDefaultProducerCreatesBaseChannel() {
        BufferAllocator allocator = mock(BufferAllocator.class);
        ArrowFlightProducer producer = new ArrowFlightProducer(flightTransport, allocator, middlewareKey, statsCollector);

        FlightServerChannel channel = producer.createChannel(newListener(), newMiddleware(), mock(FlightCallTracker.class));
        try {
            assertFalse("Default producer must not create the back-pressure channel", channel instanceof BackpressureFlightServerChannel);
            assertEquals(FlightServerChannel.class, channel.getClass());
        } finally {
            channel.close();
        }
    }

    private static ServerStreamListener newListener() {
        return mock(ServerStreamListener.class);
    }

    private static ServerHeaderMiddleware newMiddleware() {
        ServerHeaderMiddleware mw = mock(ServerHeaderMiddleware.class);
        when(mw.getCorrelationId()).thenReturn("1");
        return mw;
    }
}
