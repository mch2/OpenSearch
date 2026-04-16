/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
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
 * Tests that {@link StageExecutor} routes stages to the correct
 * {@link StageScheduler} via {@code selectScheduler}. Uses a test subclass
 * that overrides {@code selectScheduler} to inject spy schedulers, verifying
 * the router delegates all seven arguments unchanged.
 *
 * <p>Validates: Requirements 4.2, 4.3, 7.1
 */
public class StageExecutorRoutingTests extends OpenSearchTestCase {

    // ─── Task 20: testLocalStageRoutesToLocalScheduler ───────────────────

    /**
     * Dispatch a LOCAL stage through a test subclass that overrides
     * {@code selectScheduler} to return a local spy. Assert the local spy's
     * {@code schedule} was called with all seven arguments unchanged.
     *
     * Validates: Requirements 4.2, 4.3, 7.1
     */
    public void testLocalStageRoutesToLocalScheduler() {
        ClusterService clusterService = mock(ClusterService.class);

        SpyScheduler localSpy = new SpyScheduler();
        SpyScheduler fanOutSpy = new SpyScheduler();

        StageExecutor executor = new StageExecutor(clusterService) {
            @Override
            StageScheduler selectScheduler(Stage stage) {
                if (stage.getExecutionType() == StageExecutionType.LOCAL) {
                    return localSpy;
                }
                return fanOutSpy;
            }
        };

        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL);
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = (request, node, listener) -> fail("should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        AtomicBoolean success = new AtomicBoolean(false);
        ActionListener<Void> listener = ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e));

        executor.dispatch(stage, outputSink, client, childDispatcher, config, state, listener);

        assertTrue("Local spy should have been called", localSpy.called.get());
        assertFalse("Fan-out spy should NOT have been called", fanOutSpy.called.get());
        assertSame("Stage argument should pass through unchanged", stage, localSpy.capturedStage.get());
        assertSame("OutputSink argument should pass through unchanged", outputSink, localSpy.capturedSink.get());
        assertSame("Client argument should pass through unchanged", client, localSpy.capturedClient.get());
        assertSame("ChildDispatcher argument should pass through unchanged", childDispatcher, localSpy.capturedChildDispatcher.get());
        assertSame("Config argument should pass through unchanged", config, localSpy.capturedConfig.get());
        assertSame("State argument should pass through unchanged", state, localSpy.capturedState.get());
        assertSame("Listener argument should pass through unchanged", listener, localSpy.capturedListener.get());
    }

    // ─── Task 21: testDataNodeStageRoutesToShardFanOutScheduler ──────────

    /**
     * Dispatch a DATA_NODE stage. Assert the shard-fan-out spy's
     * {@code schedule} was called with all seven arguments unchanged.
     *
     * Validates: Requirements 4.2, 4.3, 7.1
     */
    public void testDataNodeStageRoutesToShardFanOutScheduler() {
        ClusterService clusterService = mock(ClusterService.class);

        SpyScheduler localSpy = new SpyScheduler();
        SpyScheduler fanOutSpy = new SpyScheduler();

        StageExecutor executor = new StageExecutor(clusterService) {
            @Override
            StageScheduler selectScheduler(Stage stage) {
                if (stage.getExecutionType() == StageExecutionType.LOCAL) {
                    return localSpy;
                }
                return fanOutSpy;
            }
        };

        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.DATA_NODE);
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = (request, node, listener) -> fail("should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        AtomicBoolean success = new AtomicBoolean(false);
        ActionListener<Void> listener = ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e));

        executor.dispatch(stage, outputSink, client, childDispatcher, config, state, listener);

        assertTrue("Fan-out spy should have been called", fanOutSpy.called.get());
        assertFalse("Local spy should NOT have been called", localSpy.called.get());
        assertSame("Stage argument should pass through unchanged", stage, fanOutSpy.capturedStage.get());
        assertSame("OutputSink argument should pass through unchanged", outputSink, fanOutSpy.capturedSink.get());
        assertSame("Client argument should pass through unchanged", client, fanOutSpy.capturedClient.get());
        assertSame("ChildDispatcher argument should pass through unchanged", childDispatcher, fanOutSpy.capturedChildDispatcher.get());
        assertSame("Config argument should pass through unchanged", config, fanOutSpy.capturedConfig.get());
        assertSame("State argument should pass through unchanged", state, fanOutSpy.capturedState.get());
        assertSame("Listener argument should pass through unchanged", listener, fanOutSpy.capturedListener.get());
    }

    // ─── Task 22: testPassThroughLocalStageRoutesToLocalScheduler ────────

    /**
     * Pass-through LOCAL stage (null fragment, no children) still routes to
     * the local scheduler. The pass-through check is internal to
     * {@link LocalStageScheduler}, not a router concern.
     *
     * Validates: Requirements 4.2
     */
    public void testPassThroughLocalStageRoutesToLocalScheduler() {
        ClusterService clusterService = mock(ClusterService.class);

        SpyScheduler localSpy = new SpyScheduler();
        SpyScheduler fanOutSpy = new SpyScheduler();

        StageExecutor executor = new StageExecutor(clusterService) {
            @Override
            StageScheduler selectScheduler(Stage stage) {
                if (stage.getExecutionType() == StageExecutionType.LOCAL) {
                    return localSpy;
                }
                return fanOutSpy;
            }
        };

        // Pass-through: null fragment, LOCAL execution type
        Stage stage = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL);
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = (request, node, listener) -> fail("should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);
        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ActionListener<Void> listener = ActionListener.wrap(v -> {}, e -> fail("unexpected: " + e));

        executor.dispatch(stage, outputSink, client, childDispatcher, config, state, listener);

        assertTrue("Local spy should have been called for pass-through LOCAL", localSpy.called.get());
        assertFalse("Fan-out spy should NOT have been called", fanOutSpy.called.get());
    }

    // ─── Task 23: testTestOnlyConstructorPassesNullBackendToLocalScheduler

    /**
     * {@code new StageExecutor(clusterService)} (test-only 1-arg ctor),
     * dispatch a compute LOCAL stage. Assert the null-backend fast-fail path
     * fires via {@link LocalStageScheduler} (IllegalStateException mentioning
     * "primary backend").
     *
     * Validates: Requirements 4.6
     */
    public void testTestOnlyConstructorPassesNullBackendToLocalScheduler() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutor executor = new StageExecutor(clusterService);

        // Compute LOCAL stage (non-pass-through): needs a non-null, non-StageInputScan fragment
        // Use a DATA_NODE child to make it non-trivial, but the key is the fragment is null
        // Actually, for pass-through detection: fragment == null → pass-through.
        // We need a non-pass-through stage. But we can't easily build a RelNode here without
        // Calcite setup. Instead, we rely on the real selectScheduler routing to LocalStageScheduler
        // which will check isPassThrough. A null-fragment stage IS pass-through, so it won't
        // hit the null-backend guard. We need a stage with children to avoid the short-circuit,
        // but pass-through with children just walks children.
        //
        // The simplest approach: dispatch a LOCAL stage that has a non-null fragment that is NOT
        // an OpenSearchStageInputScan. We can use a mock RelNode.
        org.apache.calcite.rel.RelNode mockFragment = mock(org.apache.calcite.rel.RelNode.class);
        Stage stage = new Stage(1, mockFragment, List.of(), null, StageExecutionType.LOCAL);

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = new SimpleExchangeSink();
        ShardRequestClient client = (request, node, listener) -> fail("should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);

        AtomicReference<Exception> captured = new AtomicReference<>();
        executor.dispatch(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> fail("should not succeed"), captured::set)
        );

        Exception e = captured.get();
        assertNotNull("Should have received failure", e);
        assertTrue("Should be IllegalStateException, got: " + e.getClass().getName(), e instanceof IllegalStateException);
        assertTrue(
            "Message should mention primaryBackend, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("primaryBackend")
        );
    }

    // ─── Task 24: testRouterUsesTypedFieldsNotBooleanHelpers ────────────

    /**
     * Verify the router's {@code selectScheduler} branches on
     * {@code stage.getExecutionType()} directly (not {@code stage.isShuffleWrite()}
     * or similar). Uses a test-only probe: a {@link Stage} subclass that throws
     * on {@code isShuffleWrite()} calls. The router must not touch it for the
     * MVP branches.
     *
     * Validates: Requirements 4.3
     */
    public void testRouterUsesTypedFieldsNotBooleanHelpers() {
        ClusterService clusterService = mock(ClusterService.class);
        StageExecutor executor = new StageExecutor(clusterService);

        // Probe stage that throws if isShuffleWrite() is called
        Stage probeLocal = new Stage(0, null, List.of(), null, StageExecutionType.LOCAL) {
            @Override
            public boolean isShuffleWrite() {
                throw new AssertionError("selectScheduler should NOT call isShuffleWrite() — it should branch on getExecutionType()");
            }
        };

        // selectScheduler should not throw — it reads getExecutionType(), not isShuffleWrite()
        StageScheduler selected = executor.selectScheduler(probeLocal);
        assertNotNull("Should return a scheduler for LOCAL", selected);

        // Same probe for DATA_NODE
        Stage probeDataNode = new Stage(1, null, List.of(), null, StageExecutionType.DATA_NODE) {
            @Override
            public boolean isShuffleWrite() {
                throw new AssertionError("selectScheduler should NOT call isShuffleWrite() — it should branch on getExecutionType()");
            }
        };

        StageScheduler selectedDataNode = executor.selectScheduler(probeDataNode);
        assertNotNull("Should return a scheduler for DATA_NODE", selectedDataNode);
    }

    // ─── Spy helper ─────────────────────────────────────────────────────

    /**
     * A {@link StageScheduler} spy that captures all arguments and immediately
     * completes the listener with {@code onResponse(null)}.
     */
    private static class SpyScheduler implements StageScheduler {
        final AtomicBoolean called = new AtomicBoolean(false);
        final AtomicReference<Stage> capturedStage = new AtomicReference<>();
        final AtomicReference<ExchangeSink> capturedSink = new AtomicReference<>();
        final AtomicReference<ShardRequestClient> capturedClient = new AtomicReference<>();
        final AtomicReference<ChildDispatcher> capturedChildDispatcher = new AtomicReference<>();
        final AtomicReference<QueryContext> capturedConfig = new AtomicReference<>();
        final AtomicReference<QueryState> capturedState = new AtomicReference<>();
        final AtomicReference<ActionListener<Void>> capturedListener = new AtomicReference<>();

        @Override
        public void schedule(
            Stage stage,
            ExchangeSink outputSink,
            ShardRequestClient client,
            ChildDispatcher childDispatcher,
            QueryContext config,
            QueryState state,
            ActionListener<Void> listener
        ) {
            called.set(true);
            capturedStage.set(stage);
            capturedSink.set(outputSink);
            capturedClient.set(client);
            capturedChildDispatcher.set(childDispatcher);
            capturedConfig.set(config);
            capturedState.set(state);
            capturedListener.set(listener);
            listener.onResponse(null);
        }
    }
}
