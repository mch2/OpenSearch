/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LocalStageExecution}. Validates state transitions,
 * listener signaling, and context lifecycle management.
 *
 * Validates: Requirements 4.3
 */
@SuppressWarnings("unchecked")
public class LocalStageExecutionTests extends OpenSearchTestCase {

    /**
     * start → finalizeStage with a mock ctx that fires listener.onResponse:
     * state transitions to SUCCEEDED and listener called once.
     *
     * Validates: Requirements 4.3
     */
    public void testStartToFinalizeSucceeds() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onResponse(null);
            return null;
        }).when(ctx).asyncFinalize(any());

        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        exec.finalizeStage();

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        verify(listener, times(1)).onResponse(null);
        verify(listener, never()).onFailure(any());
        verify(ctx, times(1)).asyncFinalize(any());
    }

    /**
     * start → failChildStage: state transitions to FAILED, ctx.close()
     * called, and listener receives the failure.
     *
     * Validates: Requirements 4.3
     */
    public void testFailChildStageTransitionsToFailed() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        RuntimeException cause = new RuntimeException("child exploded");
        exec.failChildStage(cause);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        verify(ctx, times(1)).close();
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertSame(cause, captor.getValue());
        verify(listener, never()).onResponse(any());
    }

    /**
     * start → cancel: state transitions to CANCELLED, ctx.close() called,
     * and listener receives TaskCancelledException.
     *
     * Validates: Requirements 4.3
     */
    public void testCancelFromRunning() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        exec.cancel("timeout");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        verify(ctx, times(1)).close();
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertTrue(captor.getValue() instanceof TaskCancelledException);
        assertEquals("timeout", captor.getValue().getMessage());
        verify(listener, never()).onResponse(any());
    }

    /**
     * Double cancel is idempotent: listener is called exactly once.
     *
     * Validates: Requirements 4.3
     */
    public void testDoubleCancelIdempotent() throws Exception {
        LocalStageContext ctx = mock(LocalStageContext.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        exec.start();
        exec.cancel("first");
        exec.cancel("second");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        verify(listener, times(1)).onFailure(any());
        verify(listener, never()).onResponse(any());
    }

    /**
     * Listener fired exactly once when both finalizeStage and cancel race.
     * We simulate the race by having asyncFinalize fire its callback on a
     * separate thread while cancel is called on the main thread.
     *
     * Validates: Requirements 4.3
     */
    public void testListenerFiredExactlyOnce() throws Exception {
        AtomicInteger onResponseCount = new AtomicInteger(0);
        AtomicInteger onFailureCount = new AtomicInteger(0);
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

        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                onResponseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCount.incrementAndGet();
            }
        };

        LocalStageExecution exec = buildExec(ctx, listener);
        exec.start();
        exec.finalizeStage();

        // Race: cancel on main thread while asyncFinalize fires on virtual thread
        Thread.ofVirtual().start(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception ignore) {}
            exec.cancel("race cancel");
            done.countDown();
        });

        assertTrue("Timed out waiting for race", done.await(10, TimeUnit.SECONDS));

        int total = onResponseCount.get() + onFailureCount.get();
        assertEquals("Listener must be fired exactly once", 1, total);
    }

    /**
     * finalizeStage is a no-op when state is not RUNNING (e.g. still CREATED).
     *
     * Validates: Requirements 4.3
     */
    public void testFinalizeStageIgnoredWhenNotRunning() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        // Don't call start() — state is still CREATED
        exec.finalizeStage();

        assertEquals(StageExecution.State.CREATED, exec.getState());
        verify(ctx, never()).asyncFinalize(any());
        verify(listener, never()).onResponse(any());
        verify(listener, never()).onFailure(any());
    }

    /**
     * getStageId and getMetrics return the expected values.
     *
     * Validates: Requirements 4.3
     */
    public void testAccessors() {
        LocalStageContext ctx = mock(LocalStageContext.class);
        ActionListener<Void> listener = mock(ActionListener.class);
        int stageId = randomIntBetween(1, 100);
        Stage stage = mockStage(stageId);
        LocalStageExecution exec = new LocalStageExecution(stage, ctx, listener, new StageMetrics(stageId));

        assertEquals(stageId, exec.getStageId());
        assertNotNull(exec.getMetrics());
        assertEquals(stageId, exec.getMetrics().getStageId());
        assertEquals(StageExecution.State.CREATED, exec.getState());
    }

    /**
     * asyncFinalize failure path: state transitions to FAILED and
     * listener receives the exception.
     *
     * Validates: Requirements 4.3
     */
    public void testFinalizeStageAsyncFailure() {
        RuntimeException asyncError = new RuntimeException("drain failed");
        LocalStageContext ctx = mock(LocalStageContext.class);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onFailure(asyncError);
            return null;
        }).when(ctx).asyncFinalize(any());

        ActionListener<Void> listener = mock(ActionListener.class);
        LocalStageExecution exec = buildExec(ctx, listener);

        exec.start();
        exec.finalizeStage();

        assertEquals(StageExecution.State.FAILED, exec.getState());
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(captor.capture());
        assertSame(asyncError, captor.getValue());
        verify(listener, never()).onResponse(any());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int stageId) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(stageId);
        when(stage.getChildStages()).thenReturn(Collections.emptyList());
        return stage;
    }

    private LocalStageExecution buildExec(LocalStageContext ctx, ActionListener<Void> listener) {
        return new LocalStageExecution(mockStage(0), ctx, listener, new StageMetrics(0));
    }
}
