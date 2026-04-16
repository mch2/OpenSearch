/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;

import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stage-level cancellation / failure tests for {@link ShardScanStageExecution}.
 * Observes outcomes through {@link StageExecution#getState()},
 * {@link StageExecution#getFailure()}, and the stage's
 * {@link StageMetrics} — there is no per-stage {@code ActionListener} anymore.
 * The synthesis of a query-level {@code TaskCancelledException} is the
 * walker's responsibility; see {@code PlanWalker.wireRootTerminalListener}.
 */
public class StageExecutionCancellationTests extends OpenSearchTestCase {

    public void testPartialFailureCapturesWrappedStageFailure() {
        int numTargets = 3;
        CancellableTask parentTask = mockParentTask(false);

        List<StreamingResponseListener<ScanResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildStageExec(numTargets, parentTask, captured);
        task.start();

        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));

        ScanResponse response = new ScanResponse(List.of("field"), Collections.singletonList(new Object[] { "value" }));
        captured.get(1).onStreamResponse(response, true);
        captured.get(2).onStreamResponse(response, true);

        assertEquals("Terminal state must be FAILED", StageExecution.State.FAILED, task.getState());
        Exception failure = task.getFailure();
        assertNotNull("Failure must be captured", failure);
        assertTrue("Failure must be a RuntimeException", failure instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", failure.getMessage().contains("Stage 0 failed"));
        assertTrue("Cause must be the original TaskCancelledException", failure.getCause() instanceof TaskCancelledException);

        assertEquals("tasksFailed must be 1", 1, task.getMetrics().getTasksFailed());
        assertEquals("tasksCompleted must be 2", 2, task.getMetrics().getTasksCompleted());
    }

    public void testAllShardsFailTransitionsToFailedAndRecordsEndTime() {
        int numTargets = 3;
        CancellableTask parentTask = mockParentTask(true);

        List<StreamingResponseListener<ScanResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildStageExec(numTargets, parentTask, captured);
        task.start();

        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onFailure(new TaskCancelledException("task cancelled"));
        }

        assertEquals("Terminal state must be FAILED", StageExecution.State.FAILED, task.getState());
        assertNotNull("Failure must be captured", task.getFailure());
        assertTrue("End time must be recorded", task.getMetrics().getEndTimeMs() > 0);
    }

    public void testFinalizeWaitsForInFlightDrain() {
        int numTargets = 3;

        List<StreamingResponseListener<ScanResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildStageExec(numTargets, mockParentTask(true), captured);
        task.start();

        ScanResponse response = new ScanResponse(List.of("field"), Collections.singletonList(new Object[] { "value" }));

        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));
        assertEquals("Still RUNNING while other tasks are in flight", StageExecution.State.RUNNING, task.getState());

        captured.get(1).onStreamResponse(response, true);
        assertEquals("Still RUNNING while one task is in flight", StageExecution.State.RUNNING, task.getState());

        captured.get(2).onStreamResponse(response, true);
        assertEquals("Terminal state after all tasks complete", StageExecution.State.FAILED, task.getState());
        assertNotNull(task.getFailure());
    }

    public void testMetricsCountFailedTasks() {
        int numTargets = 3;

        List<StreamingResponseListener<ScanResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildStageExec(numTargets, mockParentTask(false), captured);
        task.start();

        for (int i = 0; i < numTargets; i++) {
            captured.get(i).onFailure(new TaskCancelledException("task cancelled"));
        }

        assertEquals("tasksFailed must be 3", 3, task.getMetrics().getTasksFailed());
        assertEquals("tasksCompleted must be 0", 0, task.getMetrics().getTasksCompleted());
    }

    public void testNullParentTaskStillWrapsFailureAsStageFailed() {
        int numTargets = 1;

        List<StreamingResponseListener<ScanResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildStageExec(numTargets, null, captured);
        task.start();

        captured.get(0).onFailure(new TaskCancelledException("task cancelled"));

        assertEquals(StageExecution.State.FAILED, task.getState());
        Exception failure = task.getFailure();
        assertNotNull(failure);
        assertTrue("Failure must be a RuntimeException", failure instanceof RuntimeException);
        assertTrue("Message must contain 'Stage 0 failed'", failure.getMessage().contains("Stage 0 failed"));
        assertTrue("Cause must be the TaskCancelledException", failure.getCause() instanceof TaskCancelledException);
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int numTargets) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(0);
        when(stage.isShuffleWrite()).thenReturn(false);
        return stage;
    }

    private List<ShardTarget> buildTargets(int count) {
        List<ShardTarget> targets = new ArrayList<>();
        Index index = new Index("test_index", "_na_");
        for (int i = 0; i < count; i++) {
            ShardId shardId = new ShardId(index, i);
            DiscoveryNode node = mock(DiscoveryNode.class);
            targets.add(new ShardTarget(shardId, node));
        }
        return targets;
    }

    private CancellableTask mockParentTask(boolean cancelled) {
        CancellableTask parentTask = mock(CancellableTask.class);
        when(parentTask.isCancelled()).thenReturn(cancelled);
        return parentTask;
    }

    private ShardScanStageExecution buildStageExec(
        int numTargets,
        Task parentTask,
        List<StreamingResponseListener<ScanResponse>> captured
    ) {
        Stage stage = mockStage(numTargets);
        QueryContext config = QueryContext.forTest("test-query", parentTask);
        RowProducingSink sink = new RowProducingSink();
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder = target -> new FragmentExecutionRequest(
            "test-query",
            stage.getStageId(),
            target.shardId(),
            List.of()
        );
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(mock(TransportService.class), mock(ClusterService.class)) {
            @Override
            public void dispatchScan(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<ScanResponse> streamListener,
                Task parentTaskArg,
                PendingExecutions _pending
            ) {
                captured.add(streamListener);
            }
        };
        return new ShardScanStageExecution(stage, config, sink, buildTargets(numTargets), requestBuilder, dispatcher);
    }
}

