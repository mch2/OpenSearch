/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the stage-execution registry on {@link QueryState}:
 * {@code registerStageExecution}, {@code unregisterStageExecution},
 * and {@code activeStageExecutions}.
 *
 * Validates: Requirements 4.7
 */
public class QueryStateRegistryTests extends OpenSearchTestCase {

    public void testRegisterUnregisterRoundtrip() {
        QueryState state = new QueryState();
        StageExecution exec = mockExec(42);

        state.registerStageExecution(exec);
        Collection<StageExecution> active = state.activeStageExecutions();
        assertEquals(1, active.size());
        assertTrue(active.contains(exec));

        state.unregisterStageExecution(42);
        Collection<StageExecution> afterUnregister = state.activeStageExecutions();
        assertTrue(afterUnregister.isEmpty());
    }

    public void testActiveStageExecutionsSnapshot() {
        QueryState state = new QueryState();
        StageExecution exec1 = mockExec(1);
        StageExecution exec2 = mockExec(2);

        state.registerStageExecution(exec1);
        state.registerStageExecution(exec2);

        // Take a snapshot
        Collection<StageExecution> snapshot = state.activeStageExecutions();
        assertEquals(2, snapshot.size());

        // Unregister one — the snapshot should still have both (it's a copy)
        state.unregisterStageExecution(1);
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.contains(exec1));
        assertTrue(snapshot.contains(exec2));

        // But a fresh read reflects the removal
        Collection<StageExecution> fresh = state.activeStageExecutions();
        assertEquals(1, fresh.size());
        assertTrue(fresh.contains(exec2));
    }

    public void testConcurrentRegistrations() throws Exception {
        QueryState state = new QueryState();
        int threadCount = 16;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        StageExecution[] execs = new StageExecution[threadCount];
        for (int i = 0; i < threadCount; i++) {
            execs[i] = mockExec(i);
        }

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    state.registerStageExecution(execs[idx]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }

        done.await();
        Collection<StageExecution> active = state.activeStageExecutions();
        assertEquals(threadCount, active.size());
        for (StageExecution exec : execs) {
            assertTrue(active.contains(exec));
        }
    }

    public void testIdempotentUnregister() {
        QueryState state = new QueryState();
        // Unregistering a non-existent stageId should not throw
        state.unregisterStageExecution(999);
        assertTrue(state.activeStageExecutions().isEmpty());
    }

    private static StageExecution mockExec(int stageId) {
        StageExecution exec = mock(StageExecution.class);
        when(exec.getStageId()).thenReturn(stageId);
        return exec;
    }
}
