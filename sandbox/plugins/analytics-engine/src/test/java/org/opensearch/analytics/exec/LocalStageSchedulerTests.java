/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Focused unit tests for {@link LocalStageScheduler} — the scheduler
 * extracted from {@code StageExecutor.dispatchLocalStage}.
 *
 * Validates: Requirements 2.3, 2.4
 */
@SuppressWarnings("unchecked")
public class LocalStageSchedulerTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    // ─── Task 4: testPassThroughStage ───────────────────────────────────

    /**
     * Pass-through LOCAL stage (bare {@link OpenSearchStageInputScan} fragment),
     * null {@code primaryBackend}. Children are walked with the parent's
     * outputSink, listener completes normally, and the stage ID is added to
     * {@code state.completedStages()}.
     *
     * Validates: Requirements 2.3
     */
    public void testPassThroughStage() {
        LocalStageScheduler scheduler = new LocalStageScheduler(null);

        // Pass-through: bare OpenSearchStageInputScan as fragment
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("lucene")
        );
        Stage child = new Stage(0, null, List.of(), null, StageExecutionType.DATA_NODE);
        Stage stage = new Stage(1, stageInput, List.of(child), null, StageExecutionType.LOCAL);

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");

        AtomicBoolean childDispatched = new AtomicBoolean(false);
        AtomicReference<ExchangeSink> capturedSink = new AtomicReference<>();
        ChildDispatcher childDispatcher = (s, sink, c, l) -> {
            childDispatched.set(true);
            capturedSink.set(sink);
            l.onResponse(null);
        };

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Child should have been dispatched", childDispatched.get());
        assertSame("Child should receive the parent's outputSink", outputSink, capturedSink.get());
        assertTrue("Stage ID should be in completedStages", state.completedStages().contains(1));
    }

    // ─── Task 5: testComputeLocalStageWiresLocalStageContext ────────────

    /**
     * Compute LOCAL stage with 2 children. Verifies that
     * {@code createLocalStage} is called, per-child sinks are obtained,
     * {@code LocalStageExecution.start()} is called (via
     * {@code state.registerStageExecution}), and {@code asyncFinalize} is
     * called after children complete.
     *
     * Validates: Requirements 2.3
     */
    public void testComputeLocalStageWiresLocalStageContext() {
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink sinkForChild0 = mock(ExchangeSink.class);
        ExchangeSink sinkForChild1 = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(sinkForChild0);
        when(mockCtx.sinkFor(1)).thenReturn(sinkForChild1);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());

        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        LocalStageScheduler scheduler = new LocalStageScheduler(mockBackend);

        // Build two children with fragments that have row types (needed for buildChildSchemas)
        OpenSearchStageInputScan childFragment0 = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            99,
            rowType,
            List.of("lucene")
        );
        Stage child0 = new Stage(0, childFragment0, List.of(), null, StageExecutionType.DATA_NODE);
        child0.setPlanAlternatives(List.of(new StagePlan(childFragment0, "lucene")));

        OpenSearchStageInputScan childFragment1 = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            98,
            rowType,
            List.of("lucene")
        );
        Stage child1 = new Stage(1, childFragment1, List.of(), null, StageExecutionType.DATA_NODE);
        child1.setPlanAlternatives(List.of(new StagePlan(childFragment1, "lucene")));

        // Non-pass-through fragment: LogicalProject wrapping a StageInputScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(2, projectNode, List.of(child0, child1), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1, 2, 3 })));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");

        // Children complete immediately
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        verify(mockBackend).createLocalStage(any());
        verify(mockCtx).sinkFor(0);
        verify(mockCtx).sinkFor(1);
        verify(mockCtx).asyncFinalize(any());
        assertTrue("Stage ID should be in completedStages", state.completedStages().contains(2));
    }

    // ─── Task 6: testChildFailureCallsFailChildStage ────────────────────

    /**
     * When a child dispatch fails, the failure propagates through
     * {@code LocalStageExecution.failChildStage} and the listener receives
     * {@code onFailure}.
     *
     * Validates: Requirements 2.3
     */
    public void testChildFailureCallsFailChildStage() {
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink childSink = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(childSink);
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        LocalStageScheduler scheduler = new LocalStageScheduler(mockBackend);

        // One child with a fragment that has a row type
        OpenSearchStageInputScan childFragment = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            99,
            rowType,
            List.of("lucene")
        );
        Stage child = new Stage(0, childFragment, List.of(), null, StageExecutionType.DATA_NODE);
        child.setPlanAlternatives(List.of(new StagePlan(childFragment, "lucene")));

        // Non-pass-through compute stage
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(child), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");

        // Child dispatch fails
        RuntimeException childError = new RuntimeException("child exploded");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onFailure(childError);

        AtomicReference<Exception> captured = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> fail("should not succeed"), captured::set)
        );

        assertNotNull("Should have received failure", captured.get());
        assertSame("Should propagate the child error", childError, captured.get());
        verify(mockCtx).close();
    }

    // ─── Task 7: testNullPrimaryBackendFastFailsOnComputeLocal ──────────

    /**
     * Construct with {@code primaryBackend = null}, dispatch a compute LOCAL
     * stage (non-pass-through). Assert {@link IllegalStateException} with a
     * message mentioning "primary backend".
     *
     * Validates: Requirements 2.4
     */
    public void testNullPrimaryBackendFastFailsOnComputeLocal() {
        LocalStageScheduler scheduler = new LocalStageScheduler(null);

        // Non-pass-through compute stage
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);

        AtomicReference<Exception> captured = new AtomicReference<>();
        scheduler.schedule(
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

    // ─── Task 8: testNullPrimaryBackendAllowsPassThrough ────────────────

    /**
     * {@code primaryBackend = null}, pass-through LOCAL stage → no exception;
     * listener completes normally.
     *
     * Validates: Requirements 2.4
     */
    public void testNullPrimaryBackendAllowsPassThrough() {
        LocalStageScheduler scheduler = new LocalStageScheduler(null);

        // Pass-through: bare OpenSearchStageInputScan
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("lucene")
        );
        Stage stage = new Stage(1, stageInput, List.of(), null, StageExecutionType.LOCAL);

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");
        ChildDispatcher childDispatcher = (s, sink, c, l) -> l.onResponse(null);

        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> error = new AtomicReference<>();
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), error::set)
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertNull("No error expected", error.get());
        assertTrue("Stage ID should be in completedStages", state.completedStages().contains(1));
    }

    // ─── Task 9: testRegistersAndUnregistersStageExecution ──────────────

    /**
     * Assert {@code state.registerStageExecution} is called (check
     * {@code state.activeStageExecutions()} is non-empty during execution)
     * and {@code state.unregisterStageExecution} is called on terminal state
     * (check {@code state.activeStageExecutions()} is empty after completion).
     *
     * Validates: Requirements 2.3
     */
    public void testRegistersAndUnregistersStageExecution() {
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");

        AtomicBoolean registeredDuringExec = new AtomicBoolean(false);
        LocalStageContext mockCtx = mock(LocalStageContext.class);
        ExchangeSink childSink = mock(ExchangeSink.class);
        when(mockCtx.sinkFor(0)).thenReturn(childSink);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(0);
            listener.onResponse(null);
            return null;
        }).when(mockCtx).asyncFinalize(any());
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        LocalStageScheduler scheduler = new LocalStageScheduler(mockBackend);

        // One child with a fragment
        OpenSearchStageInputScan childFragment = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            99,
            rowType,
            List.of("lucene")
        );
        Stage child = new Stage(0, childFragment, List.of(), null, StageExecutionType.DATA_NODE);
        child.setPlanAlternatives(List.of(new StagePlan(childFragment, "lucene")));

        // Non-pass-through compute stage
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            RelTraitSet.createEmpty(),
            0,
            rowType,
            List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(child), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);
        QueryState state = new QueryState();
        ExchangeSink outputSink = mock(ExchangeSink.class);
        ShardRequestClient client = (request, node, listener) -> fail("client should not be called");

        // Child dispatcher that checks registration state before completing
        ChildDispatcher childDispatcher = (s, sink, c, l) -> {
            registeredDuringExec.set(state.activeStageExecutions().isEmpty() == false);
            l.onResponse(null);
        };

        AtomicBoolean success = new AtomicBoolean(false);
        scheduler.schedule(
            stage,
            outputSink,
            client,
            childDispatcher,
            config,
            state,
            ActionListener.wrap(v -> success.set(true), e -> fail("unexpected: " + e))
        );

        assertTrue("Listener should have completed successfully", success.get());
        assertTrue("Stage execution should have been registered during child dispatch", registeredDuringExec.get());
        assertTrue("Stage executions should be empty after completion", state.activeStageExecutions().isEmpty());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private RelNode buildNonPassthroughFragment(RelNode input) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        return LogicalProject.create(input, List.of(), List.of(rexBuilder.makeInputRef(input, 0)), input.getRowType());
    }
}
