/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.schedule;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link QueryExecution}'s terminal-sink close-on-failure contract.
 * Covers gaps not exercised through {@code RowProducingSink}: non-closeable sources,
 * throwing closes, and the close-fires-exactly-once invariant.
 */
public class QueryExecutionTests extends OpenSearchTestCase {

    private StageExecutionBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = new StageExecutionBuilder(mock(org.opensearch.cluster.service.ClusterService.class), null);
    }

    public void testQueryStateStartsAsCreated() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));

        assertEquals(QueryExecution.State.CREATED, qe.getState());
    }

    public void testStartTransitionsToRunning() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();

        assertEquals(QueryExecution.State.RUNNING, qe.getState());
    }

    public void testRootStageSuccessTransitionsQueryToSucceeded() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.succeed();

        assertEquals(QueryExecution.State.SUCCEEDED, qe.getState());
    }

    public void testCloseRunsTerminalSinkOnSuccess() {
        Stage rootStage = stageWithId(0);
        CountingCloseSink sink = new CountingCloseSink();
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.succeed();

        assertEquals("terminal sink closed exactly once on success", 1, sink.closeCalls.get());
    }

    public void testCancelAllAfterRootFailureKeepsFailedAsTerminal() {
        // Query was already in FAILED before cancelAll. cancelAll's CANCELLED transition
        // must be rejected by the CAS — terminal state is sticky.
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        QueryExecution qe = newQueryExecution(rootStage, ActionListener.wrap(r -> {}, e -> {}));
        qe.start();
        root.failWith(new RuntimeException("primary"));
        qe.cancelAll("late cancel");

        assertEquals("first terminal wins", QueryExecution.State.FAILED, qe.getState());
    }

    public void testCancelAllTransitionsToCancelledIdempotently() {
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, new CountingCloseSink());
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicInteger failureCount = new AtomicInteger();
        QueryExecution qe = newQueryExecution(
            rootStage,
            ActionListener.wrap(r -> fail("unexpected success"), e -> failureCount.incrementAndGet())
        );
        qe.start();
        qe.cancelAll("first");
        qe.cancelAll("second");

        assertEquals(QueryExecution.State.CANCELLED, qe.getState());
        assertEquals("listener fired exactly once across two cancelAll calls", 1, failureCount.get());
    }

    public void testFailedTransitionClosesClosableTerminalSinkExactlyOnce() {
        CountingCloseSink sink = new CountingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException rootCause = new RuntimeException("stage failed");
        root.failWith(rootCause);

        assertEquals("terminal sink close fires exactly once on FAILED", 1, sink.closeCalls.get());
        assertSame("original failure propagated to completion listener", rootCause, onFailure.get());
    }

    public void testFailedTransitionSkipsCloseWhenSourceIsNotASink() {
        // Source doesn't implement ExchangeSink — walker must skip close, still fire onFailure.
        ExchangeSource source = new ExchangeSource() {
            @Override
            public Iterable<VectorSchemaRoot> readResult() {
                return Collections.emptyList();
            }

            @Override
            public long getRowCount() {
                return 0;
            }
        };
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, source);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException rootCause = new RuntimeException("non-sink source path");
        root.failWith(rootCause);

        assertSame("original failure propagated despite non-sink source", rootCause, onFailure.get());
    }

    public void testFailedTransitionStillFiresOriginalFailureWhenTerminalSinkCloseThrows() {
        ThrowingCloseSink sink = new ThrowingCloseSink();
        Stage rootStage = stageWithId(0);
        TestRootExecution root = new TestRootExecution(rootStage, sink);
        builder.registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, s, cfg) -> root);

        AtomicReference<Exception> onFailure = new AtomicReference<>();
        newQueryExecution(rootStage, ActionListener.wrap(r -> fail("unexpected success"), onFailure::set));

        RuntimeException rootCause = new RuntimeException("original failure");
        root.failWith(rootCause);

        assertSame("original stage failure must reach the listener even when terminal sink close throws", rootCause, onFailure.get());
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private QueryExecution newQueryExecution(Stage rootStage, ActionListener<Iterable<VectorSchemaRoot>> listener) {
        QueryContext ctx = queryCtx(rootStage);
        ExecutionGraph graph = ExecutionGraph.build(ctx, builder, AbstractStageExecution::start);
        return new QueryExecution(ctx, graph, AbstractStageExecution::start, listener);
    }

    private static Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        when(stage.getExecutionType()).thenReturn(StageExecutionType.LOCAL_PASSTHROUGH);
        return stage;
    }

    private static QueryContext queryCtx(Stage rootStage) {
        AnalyticsQueryTask task = new AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "q-test",
            TaskId.EMPTY_TASK_ID,
            java.util.Map.of(),
            null
        );
        QueryDAG dag = new QueryDAG("q-test", rootStage);
        return QueryContext.forTest(dag, task);
    }

    /**
     * Minimal {@link AbstractStageExecution} + {@link DataProducer} for driving QueryExecution's
     * completion listener. Materialises one parked task so {@code start()} transitions to RUNNING
     * (instead of the empty-list short-circuit straight to SUCCEEDED) — tests drive terminal
     * transitions explicitly via {@link #failWith} / {@link #succeed}; the parked runner accepts
     * dispatch without ever signalling terminal, so dispatch can't race them.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final class TestRootExecution extends AbstractStageExecution implements DataProducer {
        private final ExchangeSource source;

        TestRootExecution(Stage stage, ExchangeSource source) {
            super(stage, "test-query", List.of(), mock(AnalyticsQueryTask.class));
            this.source = source;
            this.runner = (StageTaskRunner) (task, listener) -> {};
        }

        @Override
        protected List<StageTask> materializeTasks() {
            return List.of(
                new org.opensearch.analytics.exec.schedule.coordinator.LocalStageTask(
                    new StageTaskId(getStageId(), 0),
                    l -> l.onResponse(null)
                )
            );
        }

        @Override
        public ExchangeSource outputSource() {
            return source;
        }

        void failWith(Exception cause) {
            captureFailure(cause);
            transitionTo(State.FAILED);
        }

        void succeed() {
            transitionTo(State.SUCCEEDED);
        }
    }

    /** Sink+Source that counts close() invocations. */
    private static final class CountingCloseSink implements ExchangeSink, ExchangeSource {
        final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        @Override
        public Iterable<VectorSchemaRoot> readResult() {
            return Collections.emptyList();
        }

        @Override
        public long getRowCount() {
            return 0;
        }
    }

    /** Sink+Source whose close() throws — models a misbehaving terminal collector. */
    private static final class ThrowingCloseSink implements ExchangeSink, ExchangeSource {
        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {
            throw new IllegalStateException("close() throws by design");
        }

        @Override
        public Iterable<VectorSchemaRoot> readResult() {
            return Collections.emptyList();
        }

        @Override
        public long getRowCount() {
            return 0;
        }
    }
}
