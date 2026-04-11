/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link Scheduler.PendingExecutions} — the per-node
 * permit-based concurrency queue used by the Scheduler.
 */
public class PendingExecutionsTests extends OpenSearchTestCase {

    public void testRunImmediatelyWhenPermitsAvailable() {
        Scheduler.PendingExecutions pending = new Scheduler.PendingExecutions(2);
        AtomicInteger counter = new AtomicInteger(0);
        pending.tryRun(counter::incrementAndGet);
        assertEquals(1, counter.get());
    }

    public void testQueueWhenAtCapacity() {
        Scheduler.PendingExecutions pending = new Scheduler.PendingExecutions(1);
        AtomicInteger counter = new AtomicInteger(0);
        // First task runs immediately (takes the permit)
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(1, counter.get());
        // Second task is queued (no permits available)
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(1, counter.get()); // still 1 — second task queued
    }

    public void testFinishAndRunNextDequeuesTask() {
        Scheduler.PendingExecutions pending = new Scheduler.PendingExecutions(1);
        AtomicInteger counter = new AtomicInteger(0);
        // First task runs immediately
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(1, counter.get());
        // Second task is queued
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(1, counter.get());
        // Finish first task — second should now run
        pending.finishAndRunNext();
        assertEquals(2, counter.get());
    }

    public void testFIFOOrder() {
        Scheduler.PendingExecutions pending = new Scheduler.PendingExecutions(1);
        List<String> executionOrder = new ArrayList<>();

        // A runs immediately
        pending.tryRun(() -> executionOrder.add("A"));
        assertEquals(List.of("A"), executionOrder);

        // B and C are queued
        pending.tryRun(() -> executionOrder.add("B"));
        pending.tryRun(() -> executionOrder.add("C"));
        assertEquals(List.of("A"), executionOrder); // only A has run

        // Finish A → B should run next (FIFO)
        pending.finishAndRunNext();
        assertEquals(List.of("A", "B"), executionOrder);

        // Finish B → C should run next
        pending.finishAndRunNext();
        assertEquals(List.of("A", "B", "C"), executionOrder);
    }

    public void testMultiplePermits() {
        Scheduler.PendingExecutions pending = new Scheduler.PendingExecutions(3);
        AtomicInteger counter = new AtomicInteger(0);

        // First 3 tasks all run immediately (3 permits)
        pending.tryRun(() -> counter.incrementAndGet());
        pending.tryRun(() -> counter.incrementAndGet());
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(3, counter.get());

        // 4th task is queued (all permits taken)
        pending.tryRun(() -> counter.incrementAndGet());
        assertEquals(3, counter.get());

        // Finish one task → 4th should now run
        pending.finishAndRunNext();
        assertEquals(4, counter.get());
    }
}
