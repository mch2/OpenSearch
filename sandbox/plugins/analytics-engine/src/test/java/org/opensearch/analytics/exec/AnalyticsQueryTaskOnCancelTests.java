/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link AnalyticsQueryTask} onCancel callback mechanism.
 * Verifies that external cancellation of the task fires a registered callback,
 * enabling the scheduler to tear down in-flight stages.
 *
 * Validates: Requirements 1.1, 1.3, 1.4
 */
public class AnalyticsQueryTaskOnCancelTests extends OpenSearchTestCase {

    /**
     * Installing a callback and then cancelling the task must invoke the callback.
     * This is the primary contract: external cancellation → callback fires → scheduler
     * can call cancelActiveStages.
     *
     * Validates: Requirements 1.1, 1.3
     */
    public void testOnCancelCallbackFiresOnCancel() {
        String queryId = randomAlphaOfLength(10);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        AtomicBoolean callbackFired = new AtomicBoolean(false);
        task.setOnCancelCallback(() -> callbackFired.set(true));

        task.cancel("test");

        assertTrue("onCancel callback must fire when task is cancelled", callbackFired.get());
    }

    /**
     * The onCancel callback must be set at most once per task instance.
     * A second call to {@code setOnCancelCallback} must throw {@link IllegalStateException}
     * to guard against accidental rewiring.
     *
     * Validates: Requirements 1.5
     */
    public void testCallbackSetOnceOnly() {
        String queryId = randomAlphaOfLength(10);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        // First call succeeds
        task.setOnCancelCallback(() -> {});

        // Second call must throw IllegalStateException
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> task.setOnCancelCallback(() -> {}));
        assertTrue(ex.getMessage().contains("onCancelCallback already set"));
    }

    /**
     * A callback that throws must not cause {@code cancel()} to propagate the exception.
     * The task must still transition to cancelled state despite the callback failure.
     *
     * Validates: Requirements 1.3
     */
    public void testCallbackExceptionSwallowed() {
        String queryId = randomAlphaOfLength(10);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        task.setOnCancelCallback(() -> { throw new RuntimeException("boom"); });

        // cancel() must NOT throw despite the callback exception
        task.cancel("test");

        assertTrue("task must be in cancelled state despite callback exception", task.isCancelled());
    }

    /**
     * Cancelling a task with no callback set must be a no-op — no exception,
     * and the task still transitions to cancelled state. This covers legacy
     * test paths where no scheduler wires a callback.
     *
     * Validates: Requirements 1.4
     */
    public void testNoCallbackIsNoOp() {
        String queryId = randomAlphaOfLength(10);
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            0L,
            "transport",
            "analytics_query",
            queryId,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        // Do NOT set any onCancelCallback — leave it null

        // cancel() must NOT throw when no callback is registered
        task.cancel("test");

        assertTrue("task must be in cancelled state even without a callback", task.isCancelled());
    }

}
