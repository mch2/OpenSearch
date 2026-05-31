/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.concurrent.ExecutorService;

/**
 * {@link FlightServerChannel} variant that gates batch submission on gRPC's outbound
 * readiness, propagating producer-side back-pressure under slow consumers.
 *
 * <p>The base channel calls {@code putNext} unconditionally and relies on the eventloop's
 * queue to absorb pressure; under a slow consumer this exhausts the flight pool. This
 * subclass exposes {@link #awaitReadyOrThrow()} for the outbound handler to call from the
 * producer thread before the batch is submitted, parking on
 * {@link BackpressureStrategy#waitForListener(long)} until gRPC reports
 * {@code isReady() == true} (i.e. the per-stream outbound buffer has drained below
 * {@code setOnReadyThreshold}).
 *
 * <p>The actual {@code putNext} on the eventloop side ({@link #sendBatch}) is unchanged
 * from the base class — the gate has already been observed.
 */
class BackpressureFlightServerChannel extends FlightServerChannel {

    private static final Logger logger = LogManager.getLogger(BackpressureFlightServerChannel.class);

    private final CompositeBackpressureStrategy bp;
    private final long readyTimeoutMillis;

    public BackpressureFlightServerChannel(
        ServerStreamListener serverStreamListener,
        BufferAllocator allocator,
        ServerHeaderMiddleware middleware,
        FlightCallTracker callTracker,
        ExecutorService executor,
        long readyTimeoutMillis
    ) {
        super(serverStreamListener, allocator, middleware, callTracker, executor);
        this.readyTimeoutMillis = readyTimeoutMillis;
        // CompositeBackpressureStrategy.register replaces the cancel handler the parent
        // constructor installed; its cancelCallback runs onChannelCancelled before
        // notifying parked waiters so the cancelled state is visible on wake.
        this.bp = new CompositeBackpressureStrategy(this::onChannelCancelled);
        this.bp.register(serverStreamListener);
    }

    /**
     * Parks the calling thread until gRPC signals it can accept another batch. Called
     * from the producer thread before the batch is submitted to the channel's executor.
     *
     * @throws StreamException with {@link StreamErrorCode#TIMED_OUT} if the consumer
     *         remains not-ready longer than {@code readyTimeoutMillis}, or with
     *         {@link StreamErrorCode#CANCELLED} if the client cancelled.
     */
    public void awaitReadyOrThrow() {
        if (cancelled) {
            throw StreamException.cancelled("stream cancelled before back-pressure wait");
        }
        BackpressureStrategy.WaitResult result = bp.waitForListener(readyTimeoutMillis);
        switch (result) {
            case READY:
                return;
            case CANCELLED:
                throw StreamException.cancelled("stream cancelled while waiting for consumer");
            case TIMEOUT:
                throw new StreamException(StreamErrorCode.TIMED_OUT, "consumer not ready after " + readyTimeoutMillis + "ms");
            default:
                logger.warn("unexpected back-pressure wait result: {}", result);
                throw new StreamException(StreamErrorCode.INTERNAL, "unexpected back-pressure wait result: " + result);
        }
    }
}
