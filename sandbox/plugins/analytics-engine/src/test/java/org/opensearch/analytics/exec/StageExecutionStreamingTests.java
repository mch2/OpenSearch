/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for streaming response handling in {@link FanOutStageExecution}.
 * These tests capture the {@link StreamingResponseListener} passed to
 * {@link ShardRequestClient#send} and drive it directly with
 * {@code onStreamResponse(response, isLast)} calls.
 *
 * <p>Expected to FAIL until Task 8 updates {@code FanOutStageExecution.dispatchShardTask}
 * to use {@link StreamingResponseListener} instead of {@code ActionListener}.
 *
 * Validates: Requirements 6.1, 6.2, 6.3, 6.4
 */
@SuppressWarnings("unchecked")
public class StageExecutionStreamingTests extends OpenSearchTestCase {

    /**
     * 7.1: {@code onStreamResponse(batch, false)} feeds the sink but does NOT
     * increment {@code tasksCompleted} and does NOT call {@code onTaskCompletion}.
     *
     * Validates: Requirements 6.1, 6.2
     */
    public void testIntermediateBatchFeedsSinkNoCompletion() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> stageListener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        AtomicReference<StreamingResponseListener> capturedListener = new AtomicReference<>();
        ShardRequestClient client = (request, node, listener) -> capturedListener.set(listener);

        FanOutStageExecution task = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new SinkFeedingHandler(rootSink),
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>(),
            client,
            stageListener,
            new StageMetrics(stage.getStageId())
        );

        task.run();
        assertNotNull("StreamingResponseListener must be captured", capturedListener.get());

        // Send an intermediate batch (isLast=false)
        FragmentExecutionResponse batch = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        capturedListener.get().onStreamResponse(batch, false);

        // Sink should have been fed
        verify(rootSink, times(1)).feed(batch);
        // tasksCompleted must still be 0 — only incremented on isLast=true
        assertEquals("tasksCompleted must be 0 after intermediate batch", 0, task.getMetrics().getTasksCompleted());
        // inFlight must be unchanged (still 1) — only decremented via onTaskCompletion on isLast=true
        assertEquals("inFlight must remain 1 after intermediate batch", 1, task.getInFlight());
        // Stage listener must NOT have been called (no completion yet)
        verify(stageListener, never()).onResponse(org.mockito.ArgumentMatchers.any());
        verify(stageListener, never()).onFailure(org.mockito.ArgumentMatchers.any());
    }

    /**
     * 7.2: {@code onStreamResponse(batch, true)} feeds the sink, increments
     * {@code tasksCompleted}, and calls {@code onTaskCompletion}.
     *
     * Validates: Requirements 6.1, 6.2
     */
    public void testFinalBatchFeedsSinkAndRunsCompletion() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> stageListener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        AtomicReference<StreamingResponseListener> capturedListener = new AtomicReference<>();
        ShardRequestClient client = (request, node, listener) -> capturedListener.set(listener);

        FanOutStageExecution task = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new SinkFeedingHandler(rootSink),
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>(),
            client,
            stageListener,
            new StageMetrics(stage.getStageId())
        );

        task.run();
        assertNotNull("StreamingResponseListener must be captured", capturedListener.get());

        // Send a final batch (isLast=true)
        FragmentExecutionResponse batch = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );
        capturedListener.get().onStreamResponse(batch, true);

        // Sink should have been fed
        verify(rootSink, times(1)).feed(batch);
        // tasksCompleted must be 1
        assertEquals("tasksCompleted must be 1 after final batch", 1, task.getMetrics().getTasksCompleted());
        // Stage should be SUCCEEDED (1 target, 1 final response)
        assertEquals("State must be SUCCEEDED", StageExecution.State.SUCCEEDED, task.getState());
        // Stage listener must have been called with success
        verify(stageListener, times(1)).onResponse(null);
    }

    /**
     * 7.3: 3 intermediate batches + 1 final batch → {@code tasksCompleted == 1}.
     * Multiple intermediate batches do NOT increment the completion counter.
     *
     * Validates: Requirements 6.1, 6.2
     */
    public void testMultiBatchCountsOnce() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> stageListener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        AtomicReference<StreamingResponseListener> capturedListener = new AtomicReference<>();
        ShardRequestClient client = (request, node, listener) -> capturedListener.set(listener);

        FanOutStageExecution task = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new SinkFeedingHandler(rootSink),
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>(),
            client,
            stageListener,
            new StageMetrics(stage.getStageId())
        );

        task.run();
        assertNotNull("StreamingResponseListener must be captured", capturedListener.get());

        FragmentExecutionResponse batch = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        // 3 intermediate batches
        capturedListener.get().onStreamResponse(batch, false);
        capturedListener.get().onStreamResponse(batch, false);
        capturedListener.get().onStreamResponse(batch, false);

        assertEquals("tasksCompleted must be 0 after 3 intermediate batches", 0, task.getMetrics().getTasksCompleted());

        // 1 final batch
        capturedListener.get().onStreamResponse(batch, true);

        assertEquals("tasksCompleted must be 1 after final batch", 1, task.getMetrics().getTasksCompleted());
        // Sink fed 4 times total (3 intermediate + 1 final)
        verify(rootSink, times(4)).feed(batch);
        // Stage completed
        assertEquals("State must be SUCCEEDED", StageExecution.State.SUCCEEDED, task.getState());
        verify(stageListener, times(1)).onResponse(null);
    }

    /**
     * 7.4: For a shuffle-write stage, an intermediate metadata batch updates
     * manifests but does NOT feed the sink.
     *
     * Validates: Requirements 6.3
     */
    public void testIntermediateBatchCollectsMetadata() {
        int numTargets = 1;
        Stage stage = mockStage(numTargets);
        when(stage.isShuffleWrite()).thenReturn(true);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> stageListener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        AtomicReference<StreamingResponseListener> capturedListener = new AtomicReference<>();
        ShardRequestClient client = (request, node, listener) -> capturedListener.set(listener);

        FanOutStageExecution task = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new ManifestCollectingHandler(),
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>(),
            client,
            stageListener,
            new StageMetrics(stage.getStageId())
        );

        task.run();
        assertNotNull("StreamingResponseListener must be captured", capturedListener.get());

        // Send a metadata batch with isLast=false
        FragmentExecutionResponse metadataBatch = new FragmentExecutionResponse(Map.of("0", "path/to/file"));
        capturedListener.get().onStreamResponse(metadataBatch, false);

        // Sink must NOT be fed for shuffle-write stages
        verify(rootSink, never()).feed(org.mockito.ArgumentMatchers.any());
        // tasksCompleted must still be 0 (intermediate)
        assertEquals("tasksCompleted must be 0 after intermediate metadata batch", 0, task.getMetrics().getTasksCompleted());
        // Stage listener must NOT have been called
        verify(stageListener, never()).onResponse(org.mockito.ArgumentMatchers.any());
    }

    /**
     * 7.5: After the stage is TERMINATED, an intermediate batch is discarded —
     * the sink is NOT fed.
     *
     * Validates: Requirements 6.4
     */
    public void testIntermediateAfterTerminationDiscarded() {
        int numTargets = 2;
        int initialBatch = 2;
        // Decider terminates after 1st completion
        TerminationDecider decider = new TerminationDecider() {
            @Override
            public int initialBatchSize(int totalTargets) {
                return initialBatch;
            }

            @Override
            public boolean shouldTerminate(ExchangeSink sink, int completedTasks, int totalTasks) {
                return completedTasks >= 1;
            }
        };
        Stage stage = mockStageWithDecider(initialBatch, decider);
        List<ShardTarget> targets = buildTargets(numTargets);
        ActionListener<Void> stageListener = mock(ActionListener.class);
        ExchangeSink rootSink = mock(ExchangeSink.class);

        // Capture listeners for both targets
        List<StreamingResponseListener> capturedListeners = new ArrayList<>();
        AtomicInteger sendCount = new AtomicInteger(0);
        ShardRequestClient client = (request, node, listener) -> {
            capturedListeners.add(listener);
            sendCount.incrementAndGet();
        };

        FanOutStageExecution task = new FanOutStageExecution(
            stage,
            "test-query",
            targets,
            List.of(),
            Runnable::run,
            null,
            rootSink,
            new SinkFeedingHandler(rootSink),
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>(),
            client,
            stageListener,
            new StageMetrics(stage.getStageId())
        );

        task.run();
        assertEquals("Both targets must be dispatched", 2, sendCount.get());
        assertEquals("Must have captured 2 listeners", 2, capturedListeners.size());

        FragmentExecutionResponse batch = new FragmentExecutionResponse(
            List.of("field"),
            Collections.singletonList(new Object[] { "value" })
        );

        // Complete the first target with isLast=true → triggers termination
        capturedListeners.get(0).onStreamResponse(batch, true);

        // Stage should be terminated/succeeded now
        assertTrue(
            "State must be terminal after decider triggers",
            task.getState() == StageExecution.State.SUCCEEDED || task.getState() == StageExecution.State.TERMINATED
        );

        // Now send an intermediate batch on the second target — should be discarded
        capturedListeners.get(1).onStreamResponse(batch, false);

        // Sink should have been fed only once (from the first target's final batch)
        verify(rootSink, times(1)).feed(batch);
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage(int initialBatchSize) {
        TerminationDecider decider = mock(TerminationDecider.class);
        when(decider.initialBatchSize(org.mockito.ArgumentMatchers.anyInt())).thenReturn(initialBatchSize);
        when(
            decider.shouldTerminate(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt(),
                org.mockito.ArgumentMatchers.anyInt()
            )
        ).thenReturn(false);

        Stage stage = mock(Stage.class);
        when(stage.getTerminationDecider()).thenReturn(decider);
        when(stage.getStageId()).thenReturn(0);
        when(stage.isShuffleWrite()).thenReturn(false);
        return stage;
    }

    private Stage mockStageWithDecider(int initialBatchSize, TerminationDecider decider) {
        Stage stage = mock(Stage.class);
        when(stage.getTerminationDecider()).thenReturn(decider);
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
}
