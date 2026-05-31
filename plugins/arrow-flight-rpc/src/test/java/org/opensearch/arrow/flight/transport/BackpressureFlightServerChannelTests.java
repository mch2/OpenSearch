/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackpressureFlightServerChannelTests extends OpenSearchTestCase {

    private ServerStreamListener listener;
    private BufferAllocator allocator;
    private ServerHeaderMiddleware middleware;
    private FlightCallTracker callTracker;
    private ExecutorService executor;
    private AtomicBoolean ready;
    private AtomicBoolean listenerCancelled;
    private AtomicReference<Runnable> capturedReadyHandler;
    private AtomicReference<Runnable> capturedCancelHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        listener = mock(ServerStreamListener.class);
        allocator = mock(BufferAllocator.class);
        middleware = mock(ServerHeaderMiddleware.class);
        when(middleware.getCorrelationId()).thenReturn("42");
        callTracker = mock(FlightCallTracker.class);
        executor = Executors.newSingleThreadExecutor();

        ready = new AtomicBoolean(false);
        listenerCancelled = new AtomicBoolean(false);
        when(listener.isReady()).thenAnswer(inv -> ready.get());
        when(listener.isCancelled()).thenAnswer(inv -> listenerCancelled.get());

        capturedReadyHandler = new AtomicReference<>();
        capturedCancelHandler = new AtomicReference<>();
        doAnswer(inv -> {
            capturedReadyHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));
        doAnswer(inv -> {
            capturedCancelHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));
    }

    @Override
    public void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        super.tearDown();
    }

    private BackpressureFlightServerChannel newChannel(long readyTimeoutMillis) {
        return new BackpressureFlightServerChannel(listener, allocator, middleware, callTracker, executor, readyTimeoutMillis);
    }

    /** awaitReadyOrThrow returns immediately on the fast path when gRPC reports ready. */
    public void testAwaitReadyFastPath() {
        ready.set(true);
        BackpressureFlightServerChannel ch = newChannel(5_000);

        long start = System.nanoTime();
        ch.awaitReadyOrThrow();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue("Fast path must not park (elapsed=" + elapsedMs + "ms)", elapsedMs < 100);
    }

    /** A producer thread parked in awaitReadyOrThrow wakes when gRPC fires OnReadyHandler. */
    public void testAwaitReadyParksUntilOnReadyFires() throws Exception {
        ready.set(false);
        BackpressureFlightServerChannel ch = newChannel(30_000);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<Throwable> waiterError = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            try {
                ch.awaitReadyOrThrow();
            } catch (Throwable t) {
                waiterError.set(t);
            }
        }, "producer-waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));

        // Wait for the producer thread to actually park inside Object.wait. Polling
        // on Thread.State avoids fixed sleeps and the associated timing flakiness.
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        ready.set(true);
        capturedReadyHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());
        assertNull("awaitReadyOrThrow must return normally on READY", waiterError.get());
    }

    /** awaitReadyOrThrow throws DEADLINE_EXCEEDED when the consumer never becomes ready. */
    public void testAwaitReadyTimeoutThrowsDeadlineExceeded() {
        ready.set(false);
        BackpressureFlightServerChannel ch = newChannel(100);

        StreamException ex = expectThrows(StreamException.class, ch::awaitReadyOrThrow);
        assertEquals(StreamErrorCode.TIMED_OUT, ex.getErrorCode());
        assertTrue("Message should reference the timeout", ex.getMessage().contains("100ms"));
    }

    /**
     * Cancellation while a producer thread is parked must wake it with a CANCELLED
     * StreamException AND run the channel's onChannelCancelled cleanup (recordCallEnd,
     * close).
     */
    public void testCancelWhileWaitingThrowsAndRunsChannelCleanup() throws Exception {
        ready.set(false);
        BackpressureFlightServerChannel ch = newChannel(30_000);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<Throwable> waiterError = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            try {
                ch.awaitReadyOrThrow();
            } catch (Throwable t) {
                waiterError.set(t);
            }
        }, "producer-waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        // Composite strategy must run channel cleanup before notifying so the waiter
        // observes the cancelled state on wake.
        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());

        Throwable t = waiterError.get();
        assertNotNull("waiter must have thrown", t);
        assertTrue("must be StreamException, got " + t, t instanceof StreamException);
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) t).getErrorCode());

        // Channel cleanup must have run: callTracker.recordCallEnd(CANCELLED) was called
        // and the channel is closed.
        verify(callTracker).recordCallEnd(StreamErrorCode.CANCELLED.name());
        assertFalse("channel must be closed after cancel", ch.isOpen());
    }

    /**
     * If the channel was already cancelled before awaitReadyOrThrow is called, it must
     * throw immediately without parking.
     */
    public void testAwaitReadyOnAlreadyCancelledChannelThrows() {
        ready.set(false);
        BackpressureFlightServerChannel ch = newChannel(30_000);

        // Trigger cancel before awaitReadyOrThrow.
        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        long start = System.nanoTime();
        StreamException ex = expectThrows(StreamException.class, ch::awaitReadyOrThrow);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(StreamErrorCode.CANCELLED, ex.getErrorCode());
        assertTrue("Already-cancelled path must not park (elapsed=" + elapsedMs + "ms)", elapsedMs < 100);
    }

    /**
     * The base class's setOnCancelHandler call from the FlightServerChannel constructor
     * is overwritten by CompositeBackpressureStrategy.register. After construction, the
     * captured cancel handler must be the strategy's, not the base class's lambda.
     * Verified indirectly: when the captured cancel handler runs, it MUST invoke
     * channel cleanup (verified above via testCancelWhileWaitingThrowsAndRunsChannelCleanup).
     * This test additionally checks that the strategy registered both handlers.
     */
    public void testStrategyRegistersBothHandlers() {
        newChannel(5_000);
        assertNotNull("onReadyHandler must be installed by the strategy", capturedReadyHandler.get());
        assertNotNull("onCancelHandler must be installed by the strategy", capturedCancelHandler.get());
    }
}
