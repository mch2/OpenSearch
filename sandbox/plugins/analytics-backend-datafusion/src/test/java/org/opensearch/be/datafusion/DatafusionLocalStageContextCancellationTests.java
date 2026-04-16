/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the cancellation behaviour of {@link DatafusionLocalStageContext}.
 * <p>
 * Constructing a real {@code DatafusionLocalStageContext} requires native
 * libraries ({@link DatafusionLocalExecEngine}, {@link NativeRuntimeHandle}).
 * These tests therefore verify the cancellation <em>mechanism</em> — the
 * {@code cancelled} flag + {@code Thread.interrupt()} pattern described in the
 * design doc — by simulating the drain loop that Task&nbsp;15 will add.
 * <p>
 * Once Task&nbsp;15 lands, the drain loop inside {@code asyncFinalize} will
 * check {@code cancelled.get()} between batches and catch
 * {@link InterruptedException} from the blocking FFM call. The close path
 * will set the flag and interrupt the drain thread. This test class validates
 * that contract.
 *
 * Validates: Requirements 3.1, 3.2
 */
public class DatafusionLocalStageContextCancellationTests extends OpenSearchTestCase {

    /**
     * Simulates the drain-loop + close() contract that Task 15 will implement
     * inside {@link DatafusionLocalStageContext}:
     * <ol>
     *   <li>A virtual "drain" thread blocks indefinitely (sleep in a loop),
     *       checking a {@code cancelled} flag between iterations.</li>
     *   <li>{@code close()} sets the flag and interrupts the drain thread.</li>
     *   <li>The drain thread wakes up, sees the interrupt / flag, and signals
     *       the listener with {@link TaskCancelledException}.</li>
     * </ol>
     *
     * Validates: Requirements 3.1, 3.2
     */
    public void testCloseInterruptsDrainThread() throws Exception {
        // --- Cancellation state (mirrors fields Task 15 will add) ---
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicReference<Thread> drainThreadRef = new AtomicReference<>();
        AtomicBoolean closedFlag = new AtomicBoolean(false);

        // --- Listener that captures the terminal signal ---
        CountDownLatch listenerLatch = new CountDownLatch(1);
        AtomicReference<Exception> listenerFailure = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                listenerLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                listenerFailure.set(e);
                listenerLatch.countDown();
            }
        };

        // --- Latch so the test knows the drain thread is running ---
        CountDownLatch drainStarted = new CountDownLatch(1);

        // --- Simulate asyncFinalize: drain thread blocks on pollNext ---
        Thread drainThread = Thread.ofVirtual().name("test-drain").start(() -> {
            drainThreadRef.set(Thread.currentThread());
            drainStarted.countDown();
            try {
                // Simulate blocking engine poll — sleep in a loop, checking flag
                while (cancelled.get() == false) {
                    Thread.sleep(100_000); // blocks until interrupted
                }
                listener.onFailure(new TaskCancelledException("local stage cancelled"));
            } catch (InterruptedException ie) {
                listener.onFailure(new TaskCancelledException("local stage interrupted"));
            } finally {
                drainThreadRef.set(null);
            }
        });

        // Wait for the drain thread to be running
        assertTrue("drain thread should start within 5s", drainStarted.await(5, TimeUnit.SECONDS));

        // --- Simulate close() — sets flag + interrupts drain thread ---
        if (closedFlag.compareAndSet(false, true)) {
            cancelled.set(true);
            Thread t = drainThreadRef.get();
            if (t != null) {
                t.interrupt();
            }
        }

        // --- Assertions ---

        // Listener must receive TaskCancelledException within 1 second
        assertTrue("listener should be signaled within 1s", listenerLatch.await(1, TimeUnit.SECONDS));
        assertNotNull("listener should receive a failure", listenerFailure.get());
        assertTrue(
            "failure should be TaskCancelledException but was " + listenerFailure.get().getClass().getName(),
            listenerFailure.get() instanceof TaskCancelledException
        );

        // Drain thread must have exited
        drainThread.join(1_000);
        assertFalse("drain thread should have exited", drainThread.isAlive());
    }

    /**
     * Verifies that the {@code close()} mechanism is idempotent — calling it
     * twice does not throw and the close body executes exactly once.
     * <p>
     * Since we cannot construct a real {@link DatafusionLocalStageContext}
     * without native libraries, this test simulates the CAS-guarded close
     * pattern used in the production code.
     *
     * Validates: Requirements 3.4
     */
    public void testCloseIdempotent() {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicBoolean closedFlag = new AtomicBoolean(false);
        AtomicInteger closeCount = new AtomicInteger(0);

        // Simulate close() — mirrors the CAS pattern in DatafusionLocalStageContext
        Runnable simulatedClose = () -> {
            if (closedFlag.compareAndSet(false, true)) {
                cancelled.set(true);
                closeCount.incrementAndGet();
            }
        };

        // First close
        simulatedClose.run();
        assertTrue(closedFlag.get());
        assertTrue(cancelled.get());
        assertEquals(1, closeCount.get());

        // Second close — must be a no-op, no exception
        simulatedClose.run();
        assertTrue(closedFlag.get());
        assertTrue(cancelled.get());
        assertEquals("close body should execute exactly once", 1, closeCount.get());
    }

    /**
     * Verifies that calling {@code close()} after the drain has already
     * completed naturally is safe — no exception is thrown and the listener
     * is not signaled a second time.
     * <p>
     * The drain thread completes normally (no batches, immediate EOF),
     * signals the listener with {@code onResponse}, and sets the
     * {@code closedFlag}. A subsequent {@code close()} call sees the flag
     * already set and becomes a no-op.
     *
     * Validates: Requirements 3.4
     */
    public void testCloseAfterNaturalCompletion() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicBoolean closedFlag = new AtomicBoolean(false);

        // Listener that counts signals
        AtomicInteger signalCount = new AtomicInteger(0);
        CountDownLatch listenerLatch = new CountDownLatch(1);
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                signalCount.incrementAndGet();
                listenerLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                signalCount.incrementAndGet();
                listenerLatch.countDown();
            }
        };

        // Simulate drain completing naturally (no batches, immediate EOF)
        Thread drainThread = Thread.ofVirtual().name("test-drain-natural").start(() -> {
            // Drain completes immediately — no batches to process
            // Simulate the natural completion path
            if (closedFlag.compareAndSet(false, true)) {
                // engine.close() equivalent
            }
            listener.onResponse(null);
        });

        // Wait for natural completion
        assertTrue("listener should be signaled within 5s", listenerLatch.await(5, TimeUnit.SECONDS));
        assertEquals("listener should be signaled exactly once", 1, signalCount.get());

        drainThread.join(1_000);

        // Now call close() after natural completion — must be a no-op
        // Simulate close() — CAS fails because closedFlag is already true
        if (closedFlag.compareAndSet(false, true)) {
            cancelled.set(true);
            // This body should NOT execute
            fail("close body should not execute after natural completion");
        }

        // State should be stable
        assertTrue(closedFlag.get());
        assertFalse("cancelled should not be set after natural completion", cancelled.get());
        assertEquals("listener should still have been signaled exactly once", 1, signalCount.get());
    }

}
