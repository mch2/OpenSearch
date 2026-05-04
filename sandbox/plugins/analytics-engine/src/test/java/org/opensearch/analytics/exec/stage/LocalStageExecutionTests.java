/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LocalStageExecution} covering the placeholder
 * lifecycle: inputSink delegates to the backend sink, start closes both
 * sinks and transitions, failFromChild/cancel close the backend sink.
 */
public class LocalStageExecutionTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testStartClosesBackendSinkAndTransitionsToSucceeded() {
        CapturingSink backend = new CapturingSink();
        CapturingSink downstream = new CapturingSink();
        LocalStageExecution exec = newExecution(0, backend, downstream, List.of(7));

        exec.start();

        assertTrue("backend sink closed", backend.closed);
        // Downstream is NOT closed by start() — its lifecycle is owned by the walker,
        // which still needs to read the buffered batches via outputSource().readResult().
        assertFalse("downstream must not be closed by LocalStageExecution.start()", downstream.closed);
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    public void testInputSinkRoutesToCorrectInputIndex() {
        IndexedCapturingSink backend = new IndexedCapturingSink();
        // Two children at stageIds 7 and 9; the wrappers should pin inputIndex 0 and 1 respectively.
        LocalStageExecution exec = newExecution(2, backend, new CapturingSink(), List.of(7, 9));

        ExchangeSink wrapper7 = exec.inputSink(7);
        ExchangeSink wrapper9 = exec.inputSink(9);
        assertNotSame("wrappers are distinct", wrapper7, wrapper9);

        try (VectorSchemaRoot empty7 = VectorSchemaRoot.create(new Schema(List.of()), allocator)) {
            wrapper7.feed(empty7);
        }
        try (VectorSchemaRoot empty9 = VectorSchemaRoot.create(new Schema(List.of()), allocator)) {
            wrapper9.feed(empty9);
        }

        assertEquals(2, backend.calls.size());
        assertEquals("first call routed to inputIndex 0", Integer.valueOf(0), backend.calls.get(0));
        assertEquals("second call routed to inputIndex 1", Integer.valueOf(1), backend.calls.get(1));
    }

    public void testInputSinkRejectsUnknownChildStageId() {
        IndexedCapturingSink backend = new IndexedCapturingSink();
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));
        expectThrows(IllegalArgumentException.class, () -> exec.inputSink(42));
    }

    public void testInputSinkWrapperCloseDoesNotCloseBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));
        exec.inputSink(7).close();
        assertFalse("wrapper close must not close shared backend sink", backend.closed);
    }

    public void testOutputSourceReturnsDownstreamWhenItImplementsExchangeSource() {
        RowProducingSink downstream = new RowProducingSink();
        LocalStageExecution exec = newExecution(0, new CapturingSink(), downstream, List.of(7));
        assertSame(downstream, exec.outputSource());
    }

    public void testOutputSourceThrowsWhenDownstreamDoesNotImplementExchangeSource() {
        LocalStageExecution exec = newExecution(0, new CapturingSink(), new CapturingSink(), List.of(7));
        expectThrows(UnsupportedOperationException.class, exec::outputSource);
    }

    public void testStartTransitionsToFailedWhenCloseThrows() {
        RuntimeException boom = new RuntimeException("close blew up");
        ExchangeSink backend = new ExchangeSink() {
            @Override
            public void feed(VectorSchemaRoot batch) {}

            @Override
            public void close() {
                throw boom;
            }
        };
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));

        exec.start();

        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(boom, exec.getFailure());
    }

    public void testStartIsNoopAfterTerminalTransition() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));

        exec.cancel("test cancellation");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);

        backend.closed = false;
        exec.start();
        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertFalse("backend sink not re-closed by start()", backend.closed);
    }

    public void testFailFromChildClosesBackendSinkAndTransitions() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));

        Exception cause = new RuntimeException("child failed");
        boolean transitioned = exec.failFromChild(cause);

        assertTrue(transitioned);
        assertEquals(StageExecution.State.FAILED, exec.getState());
        assertSame(cause, exec.getFailure());
        assertTrue(backend.closed);
    }

    public void testCancelClosesBackendSink() {
        CapturingSink backend = new CapturingSink();
        LocalStageExecution exec = newExecution(0, backend, new CapturingSink(), List.of(7));

        exec.cancel("user requested");

        assertEquals(StageExecution.State.CANCELLED, exec.getState());
        assertTrue(backend.closed);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private LocalStageExecution newExecution(int stageId, ExchangeSink backend, ExchangeSink downstream, List<Integer> childStageIds) {
        Schema emptySchema = new Schema(List.of());
        List<ExchangeSinkContext.InputDescriptor> inputs = new ArrayList<>(childStageIds.size());
        for (int i = 0; i < childStageIds.size(); i++) {
            inputs.add(new ExchangeSinkContext.InputDescriptor(childStageIds.get(i), "input-" + i, emptySchema));
        }
        ExchangeSinkContext ctx = new ExchangeSinkContext("q-test", stageId, new byte[0], allocator, inputs, downstream);
        return new LocalStageExecution(stageWithId(stageId), backend, ctx, downstream);
    }

    private Stage stageWithId(int id) {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(id);
        when(stage.getChildStages()).thenReturn(List.of());
        return stage;
    }

    /** ExchangeSink that records feed/close calls and releases held batches on close. */
    private static final class CapturingSink implements ExchangeSink {
        final List<VectorSchemaRoot> fed = new ArrayList<>();
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            fed.add(batch);
        }

        @Override
        public void close() {
            closed = true;
            for (VectorSchemaRoot batch : fed) {
                batch.close();
            }
        }
    }

    /** ExchangeSink that records the inputIndex of each multi-input feed call. */
    private static final class IndexedCapturingSink implements ExchangeSink {
        final List<Integer> calls = new ArrayList<>();
        boolean closed = false;

        @Override
        public void feed(VectorSchemaRoot batch) {
            // Single-input path — rejected because LocalStageExecution wrappers always call feed(int, batch).
            throw new UnsupportedOperationException("IndexedCapturingSink expects feed(int, batch)");
        }

        @Override
        public void feed(int inputIndex, VectorSchemaRoot batch) {
            calls.add(inputIndex);
            batch.close();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
