/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the {@code cancelActiveStages} helper in {@link DefaultPlanExecutor}.
 * Exercises the static helper directly (package-private access) against a real
 * {@link QueryState} with mock {@link StageExecution} instances.
 *
 * Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5
 */
public class DefaultPlanExecutorCancellationTests extends OpenSearchTestCase {

    /**
     * Register 3 mock stage executions, call cancelActiveStages, verify cancel()
     * was called on each with the expected reason string.
     *
     * Validates: Requirements 5.1
     */
    public void testFailurePathCancelsActiveStages() {
        QueryState state = new QueryState();
        List<RecordingStageExecution> execs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            RecordingStageExecution exec = new RecordingStageExecution(i);
            execs.add(exec);
            state.registerStageExecution(exec);
        }

        String reason = "query failed: something broke";
        DefaultPlanExecutor.cancelActiveStages(state, reason);

        for (RecordingStageExecution exec : execs) {
            assertEquals("cancel should be called exactly once on stage " + exec.getStageId(), 1, exec.cancelCount.get());
            assertEquals(reason, exec.lastReason);
        }
    }

    /**
     * Verify that cancelActiveStages is called BEFORE closeBufferAllocator and
     * taskManager.unregister in the failure path. We test this by confirming the
     * ordering contract: stages are still in the registry when cancel fires
     * (i.e., cancel sees them before they are unregistered).
     *
     * Validates: Requirements 5.2
     */
    public void testCancelBeforeUnregister() {
        QueryState state = new QueryState();

        // A stage execution that records whether it was still in the registry at cancel time
        AtomicInteger registrySizeAtCancel = new AtomicInteger(-1);
        StageExecution exec = new StageExecution() {
            @Override
            public int getStageId() {
                return 42;
            }

            @Override
            public State getState() {
                return State.RUNNING;
            }

            @Override
            public StageMetrics getMetrics() {
                return new StageMetrics(42);
            }

            @Override
            public void cancel(String reason) {
                // At cancel time, the stage should still be in the registry
                registrySizeAtCancel.set(state.activeStageExecutions().size());
            }
        };

        state.registerStageExecution(exec);

        // Simulate the failure path ordering from DefaultPlanExecutor:
        // 1. cancelActiveStages (stages still registered)
        DefaultPlanExecutor.cancelActiveStages(state, "query failed: test");
        // 2. then unregister would happen (closeBufferAllocator + taskManager.unregister)
        state.unregisterStageExecution(42);

        assertEquals("Stage should still be in registry when cancel() is called", 1, registrySizeAtCancel.get());
        assertTrue("Stage should be unregistered after cancel path completes", state.activeStageExecutions().isEmpty());
    }

    /**
     * Calling cancelActiveStages twice is safe — already-cancelled stages
     * receive a second cancel() call which is idempotent (no exception).
     *
     * Validates: Requirements 5.3
     */
    public void testIdempotentDoubleCancel() {
        QueryState state = new QueryState();
        RecordingStageExecution exec = new RecordingStageExecution(7);
        state.registerStageExecution(exec);

        DefaultPlanExecutor.cancelActiveStages(state, "first cancel");
        DefaultPlanExecutor.cancelActiveStages(state, "second cancel");

        // cancel() called twice — both should succeed without exception
        assertEquals(2, exec.cancelCount.get());
        assertEquals("second cancel", exec.lastReason);
    }

    /**
     * If a stage's cancel() throws an exception, cancelActiveStages swallows it
     * and continues cancelling the remaining stages.
     *
     * Validates: Requirements 5.4
     */
    public void testCancelIgnoresExceptions() {
        QueryState state = new QueryState();

        // First stage throws on cancel
        StageExecution throwingExec = new StageExecution() {
            @Override
            public int getStageId() {
                return 1;
            }

            @Override
            public State getState() {
                return State.RUNNING;
            }

            @Override
            public StageMetrics getMetrics() {
                return new StageMetrics(1);
            }

            @Override
            public void cancel(String reason) {
                throw new RuntimeException("cancel exploded");
            }
        };

        // Second stage records the cancel normally
        RecordingStageExecution normalExec = new RecordingStageExecution(2);

        state.registerStageExecution(throwingExec);
        state.registerStageExecution(normalExec);

        // Should not throw despite the first stage's cancel() throwing
        DefaultPlanExecutor.cancelActiveStages(state, "query failed: boom");

        assertEquals("Normal stage should still get cancelled", 1, normalExec.cancelCount.get());
    }

    /**
     * Calling cancelActiveStages on an empty registry is a no-op — no exception.
     *
     * Validates: Requirements 5.1
     */
    public void testCancelWithEmptyRegistryIsNoop() {
        QueryState state = new QueryState();
        // Should not throw
        DefaultPlanExecutor.cancelActiveStages(state, "no stages to cancel");
    }

    /**
     * Stages that have already been unregistered (naturally terminated) are not
     * visible to cancelActiveStages — only active stages are cancelled.
     *
     * Validates: Requirements 5.3, 5.5
     */
    public void testAlreadyTerminatedStagesNotCancelled() {
        QueryState state = new QueryState();
        RecordingStageExecution active = new RecordingStageExecution(1);
        RecordingStageExecution terminated = new RecordingStageExecution(2);

        state.registerStageExecution(active);
        state.registerStageExecution(terminated);

        // Simulate stage 2 completing naturally and being unregistered
        state.unregisterStageExecution(2);

        DefaultPlanExecutor.cancelActiveStages(state, "query failed");

        assertEquals("Active stage should be cancelled", 1, active.cancelCount.get());
        assertEquals("Terminated (unregistered) stage should not be cancelled", 0, terminated.cancelCount.get());
    }

    /**
     * Simulate the wiring that {@code DefaultPlanExecutor.execute()} should perform
     * after {@code taskManager.register}: set the task's {@code onCancelCallback} to
     * a lambda that invokes {@code cancelActiveStages}. Verify the callback is set
     * exactly once — a second {@code setOnCancelCallback} call throws
     * {@code IllegalStateException}.
     *
     * Validates: Requirements 2.1
     */
    public void testCancellationCallbackWiredOnRegister() {
        QueryState state = new QueryState();
        RecordingStageExecution exec = new RecordingStageExecution(1);
        state.registerStageExecution(exec);

        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        // Simulate what DefaultPlanExecutor.execute() should do after taskManager.register
        task.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (task.getReasonCancelled() != null ? task.getReasonCancelled() : "unknown");
            DefaultPlanExecutor.cancelActiveStages(state, reason);
        });

        // Verify callback is set exactly once — second set throws
        expectThrows(IllegalStateException.class, () -> task.setOnCancelCallback(() -> {}));
    }

    /**
     * Simulate the full cancellation chain: external cancel → onCancelled() →
     * callback → cancelActiveStages → stage.cancel(reason).
     * Wire a {@link RecordingStageExecution} into a {@link QueryState}, set the
     * task's onCancelCallback to invoke {@code cancelActiveStages}, then fire
     * {@code task.cancel("test reason")} and verify the stage received the cancel
     * with a reason containing "task cancelled".
     *
     * Validates: Requirements 2.1, 2.2
     */
    public void testCancellationFiringTriggersCancelActiveStages() {
        QueryState state = new QueryState();
        RecordingStageExecution exec = new RecordingStageExecution(1);
        state.registerStageExecution(exec);

        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        // Wire the callback exactly as DefaultPlanExecutor.execute() will do
        task.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (task.getReasonCancelled() != null ? task.getReasonCancelled() : "unknown");
            DefaultPlanExecutor.cancelActiveStages(state, reason);
        });

        // Fire external cancellation — triggers onCancelled() → callback → cancelActiveStages
        task.cancel("test reason");

        assertEquals("cancel should be called exactly once on the stage", 1, exec.cancelCount.get());
        assertTrue("reason should contain 'task cancelled'", exec.lastReason.contains("task cancelled"));
    }

    /**
     * Wire up a {@link QueryState} with a mock stage that, on cancel, signals a
     * top-level {@link PlainActionFuture} with {@link TaskCancelledException}.
     * Fire {@code task.cancel("user requested cancel")} and assert the future
     * completes exceptionally with {@code TaskCancelledException}.
     *
     * Validates: Requirements 2.4
     */
    public void testTopLevelListenerReceivesTaskCancelledException() {
        QueryState state = new QueryState();
        PlainActionFuture<Void> future = new PlainActionFuture<>();

        // A stage execution that signals the top-level future on cancel
        // (simulating the real listener chain: stage.cancel → listener.onFailure → top-level future)
        StageExecution exec = new StageExecution() {
            @Override
            public int getStageId() {
                return 1;
            }

            @Override
            public State getState() {
                return State.RUNNING;
            }

            @Override
            public StageMetrics getMetrics() {
                return new StageMetrics(1);
            }

            @Override
            public void cancel(String reason) {
                future.onFailure(new TaskCancelledException(reason));
            }
        };
        state.registerStageExecution(exec);

        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        // Wire the callback
        task.setOnCancelCallback(() -> {
            String reason = "task cancelled: " + (task.getReasonCancelled() != null ? task.getReasonCancelled() : "unknown");
            DefaultPlanExecutor.cancelActiveStages(state, reason);
        });

        // Fire external cancellation
        task.cancel("user requested cancel");

        // The future should complete with TaskCancelledException
        assertTrue(future.isDone());
        TaskCancelledException ex = expectThrows(TaskCancelledException.class, future::actionGet);
        assertTrue(ex.getMessage().contains("task cancelled"));
    }

    // ─── Test helper ────────────────────────────────────────────────────

    /**
     * Minimal {@link StageExecution} that records cancel() invocations.
     */
    private static class RecordingStageExecution implements StageExecution {
        final int stageId;
        final AtomicInteger cancelCount = new AtomicInteger(0);
        volatile String lastReason;

        RecordingStageExecution(int stageId) {
            this.stageId = stageId;
        }

        @Override
        public int getStageId() {
            return stageId;
        }

        @Override
        public State getState() {
            return State.RUNNING;
        }

        @Override
        public StageMetrics getMetrics() {
            return new StageMetrics(stageId);
        }

        @Override
        public void cancel(String reason) {
            cancelCount.incrementAndGet();
            lastReason = reason;
        }
    }
}
