/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LocalStageExecution}. Validates state transitions,
 * {@link LocalStageContext} lifecycle management, and that the execution
 * fires its state listeners correctly. The execution no longer has a
 * per-stage {@code ActionListener} — observers run through
 * {@link StageStateListener} transitions.
 */
@SuppressWarnings("unchecked")
public class LocalStageExecutionTests extends OpenSearchTestCase {

    /**
     * {@code start()} with a mock ctx that fires its async callback
     * synchronously: state transitions through RUNNING to SUCCEEDED in one
     * call and the state listener observes both transitions.
     */
    public void testStartSucceeds() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onResponse(null);
            return null;
        }).when(ctx).asyncFinalize(any());

        LocalStageExecution exec = buildExec(ctx);
        exec.start();

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertNull("getFailure() must be null on success", exec.getFailure());
        verify(ctx, times(1)).asyncFinalize(any());
    }

    /**
     * {@code start → failFromChild}: state transitions to FAILED,
     * {@code ctx.close()} is called, and {@code getFailure()} returns the cause.
     */
    public void testFailFromChildTransitionsToFailed() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        RuntimeException cause = new RuntimeException("child exploded");
        exec.failFromChild(cause);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(cause, exec.getFailure());
        verify(ctx, times(1)).close();
    }

    /**
     * {@code start → cancel}: state transitions to CANCELLED and
     * {@code ctx.close()} is called.
     */
    public void testCancelFromRunning() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        exec.cancel("timeout");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertNull("Cancellation is not a failure — getFailure() must be null", exec.getFailure());
        verify(ctx, times(1)).close();
    }

    /**
     * Double cancel is idempotent: only one CANCELLED transition fires, and
     * {@code ctx.close()} is called exactly once.
     */
    public void testDoubleCancelIdempotent() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        AtomicInteger cancelled = new AtomicInteger(0);
        exec.addStateListener((from, to) -> {
            if (to == StageExecution.State.CANCELLED) cancelled.incrementAndGet();
        });

        exec.start();
        exec.cancel("first");
        exec.cancel("second");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertEquals("Second cancel must be a no-op", 1, cancelled.get());
        verify(ctx, times(1)).close();
    }

    /**
     * Terminal transition is fired exactly once when an asynchronous finalize
     * callback races with an external {@code cancel}. The CAS-based state
     * machine guarantees that only one of {@code SUCCEEDED} or
     * {@code CANCELLED} wins.
     */
    public void testTerminalTransitionFiresExactlyOnceUnderRace() throws Exception {
        AtomicInteger terminalCount = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(2);

        LocalStageContext ctx = mock(LocalStageContext.class);
        CyclicBarrier barrier = new CyclicBarrier(2);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            Thread.ofVirtual().start(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                } catch (Exception ignore) {}
                finalizeListener.onResponse(null);
                done.countDown();
            });
            return null;
        }).when(ctx).asyncFinalize(any());

        LocalStageExecution exec = buildExec(ctx);
        exec.addStateListener((from, to) -> {
            if (to == StageExecution.State.SUCCEEDED
                || to == StageExecution.State.FAILED
                || to == StageExecution.State.CANCELLED) {
                terminalCount.incrementAndGet();
            }
        });

        exec.start();

        // Race: cancel on a virtual thread while asyncFinalize fires on another virtual thread
        Thread.ofVirtual().start(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception ignore) {}
            exec.cancel("race cancel");
            done.countDown();
        });

        assertTrue("Timed out waiting for race", done.await(10, TimeUnit.SECONDS));
        assertEquals("Terminal transition must fire exactly once", 1, terminalCount.get());
    }

    /**
     * Double {@code start()} is idempotent: the second call is a no-op,
     * and {@code asyncFinalize} is invoked only once.
     */
    public void testDoubleStartIdempotent() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        exec.start();
        exec.start();

        verify(ctx, times(1)).asyncFinalize(any());
    }

    /**
     * {@code getStageId} and {@code getMetrics} return the expected values.
     */
    public void testAccessors() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        int stageId = randomIntBetween(1, 100);
        Stage stage = mockStage(stageId);
        LocalStageExecution exec = new LocalStageExecution(stage, ctx);

        assertEquals(stageId, exec.getStageId());
        assertNotNull(exec.getMetrics());
        assertEquals(stageId, exec.getMetrics().getStageId());
        assertEquals(StageExecution.State.CREATED, exec.getState());
    }

    /**
     * {@code asyncFinalize} failure path: state transitions to FAILED and
     * {@code getFailure()} returns the async error.
     */
    public void testStartAsyncFailure() {
        RuntimeException asyncError = new RuntimeException("drain failed");
        LocalStageContext ctx = mock(LocalStageContext.class);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onFailure(asyncError);
            return null;
        }).when(ctx).asyncFinalize(any());

        LocalStageExecution exec = buildExec(ctx);

        exec.start();

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(asyncError, exec.getFailure());
    }

    /**
     * {@code start()} with a synchronously-firing {@code asyncFinalize}: the
     * state listener observes both (CREATED → RUNNING) and (RUNNING → SUCCEEDED)
     * transitions within one {@code start()} call.
     */
    public void testStateListenerFiresOnSucceeded() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onResponse(null);
            return null;
        }).when(ctx).asyncFinalize(any());

        LocalStageExecution exec = buildExec(ctx);

        List<StageExecution.State[]> transitions = new ArrayList<>();
        exec.addStateListener((from, to) -> transitions.add(new StageExecution.State[] { from, to }));

        exec.start();

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        boolean sawRunningToSucceeded = false;
        for (StageExecution.State[] pair : transitions) {
            if (pair[0] == StageExecution.State.RUNNING && pair[1] == StageExecution.State.SUCCEEDED) {
                sawRunningToSucceeded = true;
            }
        }
        assertTrue("Expected (RUNNING, SUCCEEDED) transition", sawRunningToSucceeded);
    }

    /**
     * {@code start → failFromChild}: state listener observes (RUNNING, FAILED)
     * and {@code getFailure()} is non-null.
     */
    public void testStateListenerFiresOnFailed() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        List<StageExecution.State[]> transitions = new ArrayList<>();
        exec.addStateListener((from, to) -> transitions.add(new StageExecution.State[] { from, to }));

        exec.start();
        RuntimeException cause = new RuntimeException("test");
        exec.failFromChild(cause);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        boolean sawRunningToFailed = false;
        for (StageExecution.State[] pair : transitions) {
            if (pair[0] == StageExecution.State.RUNNING && pair[1] == StageExecution.State.FAILED) {
                sawRunningToFailed = true;
            }
        }
        assertTrue("Expected (RUNNING, FAILED) transition", sawRunningToFailed);
        assertNotNull("getFailure() should be non-null after failure", exec.getFailure());
    }

    /**
     * {@code start → cancel}: state listener observes a transition to
     * {@code CANCELLED} and {@code getFailure()} is null because
     * cancellation is not a failure.
     */
    public void testStateListenerFiresOnCancelled() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        LocalStageExecution exec = buildExec(ctx);

        List<StageExecution.State[]> transitions = new ArrayList<>();
        exec.addStateListener((from, to) -> transitions.add(new StageExecution.State[] { from, to }));

        exec.start();
        exec.cancel("test");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        boolean sawCancelled = false;
        for (StageExecution.State[] pair : transitions) {
            if (pair[1] == StageExecution.State.CANCELLED) {
                sawCancelled = true;
            }
        }
        assertTrue("Expected transition to CANCELLED", sawCancelled);
        assertNull("getFailure() should be null for cancellation", exec.getFailure());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        when(stage.getChildStages()).thenReturn(Collections.emptyList());
        return stage;
    }

    private LocalStageExecution buildExec(LocalStageContext ctx) {
        return new LocalStageExecution(mockStage(0), ctx);
    }
}
