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
import org.opensearch.arrow.flight.stats.FlightStatsCollector;

/**
 * {@link ArrowFlightProducer} variant that creates {@link BackpressureFlightServerChannel}
 * instances, enabling producer-side back-pressure on the native Arrow path. Selected by
 * {@code arrow.flight.producer.backpressure.enabled}.
 */
class BackpressureArrowFlightProducer extends ArrowFlightProducer {

    private final long readyTimeoutMillis;

    BackpressureArrowFlightProducer(
        FlightTransport flightTransport,
        BufferAllocator allocator,
        FlightServerMiddleware.Key<ServerHeaderMiddleware> middlewareKey,
        FlightStatsCollector statsCollector,
        long readyTimeoutMillis
    ) {
        super(flightTransport, allocator, middlewareKey, statsCollector);
        this.readyTimeoutMillis = readyTimeoutMillis;
    }

    @Override
    protected FlightServerChannel createChannel(
        ServerStreamListener listener,
        ServerHeaderMiddleware middleware,
        FlightCallTracker callTracker
    ) {
        return new BackpressureFlightServerChannel(
            listener,
            allocator,
            middleware,
            callTracker,
            flightTransport.getNextFlightExecutor(),
            readyTimeoutMillis
        );
    }
}
