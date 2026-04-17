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
import org.opensearch.analytics.exec.MockFragmentResponse;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;

import org.opensearch.analytics.backend.ExchangeSink;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ShardScanStageExecution#cancel(String)}. Validates that
 * cancel transitions state to {@code CANCELLED}, fires a single state
 * transition, is idempotent, and causes in-flight responses to be ignored.
 *
 * <p>The execution no longer exposes a per-stage
 * {@link org.opensearch.core.action.ActionListener} — downstream state
 * observation runs through {@link StageStateListener} transitions. These
 * tests observe cancel behaviour through {@link StageExecution#getState()}
 * and a recording state listener.
 */
@SuppressWarnings("unchecked")
public class FanOutStageExecutionCancelTests extends OpenSearchTestCase {

    public void testCancelFromRunningTransitionsAndFiresStateListener() {
        int numTargets = 3;
        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildExec(numTargets, captured);

        AtomicInteger cancelledTransitions = new AtomicInteger(0);
        task.addStateListener((from, to) -> {
            if (to == StageExecution.State.CANCELLED) cancelledTransitions.incrementAndGet();
        });

        task.start();
        assertEquals(StageExecution.State.RUNNING, task.getState());

        task.cancel("test reason");

        assertEquals("State must be CANCELLED", StageExecution.State.CANCELLED, task.getState());
        assertEquals("Exactly one CANCELLED transition must have fired", 1, cancelledTransitions.get());
    }

    public void testDoubleCancelIdempotent() {
        int numTargets = 3;
        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildExec(numTargets, captured);

        AtomicInteger cancelledTransitions = new AtomicInteger(0);
        task.addStateListener((from, to) -> {
            if (to == StageExecution.State.CANCELLED) cancelledTransitions.incrementAndGet();
        });

        task.start();
        task.cancel("first cancel");
        task.cancel("second cancel");

        assertEquals(StageExecution.State.CANCELLED, task.getState());
        assertEquals("Second cancel must be a no-op", 1, cancelledTransitions.get());
    }

    public void testInFlightResponsesAfterCancelIgnored() {
        int numTargets = 3;
        ExchangeSink rootSink = mock(ExchangeSink.class);
        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildExec(numTargets, rootSink, captured);

        task.start();
        assertEquals(numTargets, captured.size());

        task.cancel("cancelled");
        assertEquals(StageExecution.State.CANCELLED, task.getState());

        FragmentExecutionResponse response = MockFragmentResponse.create(List.of("field"), Collections.singletonList(new Object[] { "value" }));
        for (StreamingResponseListener<FragmentExecutionResponse> srl : captured) {
            srl.onStreamResponse(response, true);
        }

        // Late responses must not be fed into the sink after cancellation.
        verify(rootSink, never()).feed(any());
    }

    public void testCancelFromCreatedState() {
        int numTargets = 3;
        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        ShardScanStageExecution task = buildExec(numTargets, captured);

        assertEquals(StageExecution.State.CREATED, task.getState());

        task.cancel("cancelled before run");

        assertEquals(StageExecution.State.CANCELLED, task.getState());
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
            when(node.getId()).thenReturn("node_" + i);
            targets.add(new ShardTarget(shardId, node));
        }
        return targets;
    }

    private static Function<ShardTarget, FragmentExecutionRequest> requestFn(Stage stage) {
        return target -> new FragmentExecutionRequest("test-query", stage.getStageId(), target.shardId(), List.of());
    }

    private static AnalyticsSearchTransportService capturingDispatcher(List<StreamingResponseListener<FragmentExecutionResponse>> captured) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), mock(ClusterService.class)) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> streamListener,
                Task parentTask,
                PendingExecutions _pending
            ) {
                captured.add(streamListener);
            }
        };
    }

    private ShardScanStageExecution buildExec(int numTargets, List<StreamingResponseListener<FragmentExecutionResponse>> captured) {
        Stage stage = mockStage(numTargets);
        QueryContext config = QueryContext.forTest("test-query", null);
        RowProducingSink sinkForExec = new RowProducingSink();
        return new ShardScanStageExecution(
            stage,
            config,
            sinkForExec,
            buildTargets(numTargets),
            requestFn(stage),
            capturingDispatcher(captured)
        );
    }

    private ShardScanStageExecution buildExec(
        int numTargets,
        ExchangeSink rootSink,
        List<StreamingResponseListener<FragmentExecutionResponse>> captured
    ) {
        Stage stage = mockStage(numTargets);
        QueryContext config = QueryContext.forTest("test-query", null);
        return new ShardScanStageExecution(
            stage,
            config,
            rootSink,
            buildTargets(numTargets),
            requestFn(stage),
            capturingDispatcher(captured)
        );
    }
}

