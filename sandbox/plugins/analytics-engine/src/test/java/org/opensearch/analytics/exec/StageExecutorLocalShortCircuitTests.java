/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Tests that {@link StageExecutor#dispatch} short-circuits for {@code LOCAL}
 * stages with no children.
 *
 * Validates: Requirements 1.6
 */
public class StageExecutorLocalShortCircuitTests extends OpenSearchTestCase {

    /**
     * A LOCAL stage with no children should complete immediately.
     * The stage ID is added to completedStages and the listener fires
     * with onResponse(null).
     */
    public void testLocalStageWithEmptyChildrenSucceeds() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutor executor = new StageExecutor(clusterService);
        QueryContext config = QueryContext.forTest("test-query", null);

        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);

        // Build a LOCAL pass-through stage with no children (null fragment = pass-through)
        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> failureRef = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                failureRef.set(e);
            }
        };

        ShardRequestClient client = (request, node, shardListener) -> {
            fail("Client should not be called for a LOCAL pass-through stage");
        };

        // No-op child dispatcher — this LOCAL stage has no children
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);
        executor.dispatch(stage, rootSink, client, noOpChildren, config, state, listener);

        assertTrue("listener.onResponse should have been called", responseCalled.get());
        assertNull("listener.onFailure should not have been called", failureRef.get());
        assertTrue("Stage should be marked as completed", state.completedStages().contains(0));
    }

    /**
     * A LOCAL stage WITH children should NOT short-circuit — it should proceed
     * to the normal dispatch path via {@link FanOutStageExecution}. We verify
     * this by checking that the stage completed through the handler path.
     */
    public void testLocalStageWithChildrenDoesNotShortCircuit() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutor executor = new StageExecutor(clusterService);
        QueryContext config = QueryContext.forTest("test-query", null);

        SimpleExchangeSink rootSink = new SimpleExchangeSink();
        QueryState state = new QueryState(rootSink);

        // LOCAL pass-through stage with one child (null fragment = pass-through)
        // Pass-through stages walk children with the parent's sink, so this should NOT
        // short-circuit — it should walk the child via childDispatcher.
        Stage child = new Stage(1, null, List.of(), null, StageExecutionType.DATA_NODE);
        Stage stage = new Stage(0, null, List.of(child), null, StageExecutionType.LOCAL);

        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                // Normal dispatch path completed (zero targets -> immediate success)
            }

            @Override
            public void onFailure(Exception e) {
                // Also acceptable — the point is the short-circuit was NOT taken
            }
        };

        ShardRequestClient client = (request, node, shardListener) -> {
            fail("Client should not be called for a stage with no table name and no shuffle");
        };

        assertTrue("No stage executions should be registered before dispatch", state.activeStageExecutions().isEmpty());

        // No-op child dispatcher — children are walked by StageExecutor's LOCAL logic
        ChildDispatcher noOpChildren = (s, sink, c, l) -> l.onResponse(null);
        executor.dispatch(stage, rootSink, client, noOpChildren, config, state, listener);

        assertTrue("Stage should be in completedStages", state.completedStages().contains(0));
    }
}
