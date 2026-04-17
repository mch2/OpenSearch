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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ShardFragmentStageExecution} — verifies the state
 * machine and listener contract without real Flight transport.
 *
 * <p>Uses a mock {@link AnalyticsSearchTransportService} that captures the
 * {@link StreamingResponseListener} passed into {@code dispatchFragment}, so
 * the test body can drive the listener directly with constructed VSR batches
 * and assert on state transitions + sink feeds.
 */
public class ShardFragmentStageExecutionTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private RecordingSink sink;
    private AnalyticsQueryTask parentTask;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
        sink = new RecordingSink();
        parentTask = new AnalyticsQueryTask(1, "transport", "test_action", "q1", null, null, null);
    }

    @Override
    public void tearDown() throws Exception {
        sink.close();           // release any batches the sink is holding
        if (allocator != null) allocator.close();
        super.tearDown();
    }

    public void testEmptyTargetsTransitionsToSucceeded() {
        ShardFragmentStageExecution exec = newExec(List.of(), mock(AnalyticsSearchTransportService.class));
        exec.start();
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertEquals("no batches fed", 0, sink.batches.size());
    }

    public void testSingleShardStreamsBatchesThenSucceeds() {
        AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> captured = new AtomicReference<>();
        AnalyticsSearchTransportService dispatcher = mockDispatcher(captured);

        ShardFragmentStageExecution exec = newExec(List.of(mockTarget("nodeA")), dispatcher);
        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        // Drive the listener: 2 data batches then a completion signal (null + isLast=true).
        VectorSchemaRoot b1 = newIntRoot("x", 3);
        VectorSchemaRoot b2 = newIntRoot("x", 7);
        captured.get().onStreamResponse(new FragmentExecutionResponse(b1), false);
        captured.get().onStreamResponse(new FragmentExecutionResponse(b2), false);
        captured.get().onStreamResponse(null, true);

        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertEquals("both batches fed into sink", 2, sink.batches.size());
        assertEquals("batch-1 row count", 3, sink.batches.get(0).getRowCount());
        assertEquals("batch-2 row count", 7, sink.batches.get(1).getRowCount());
    }

    public void testShardFailureTransitionsToFailedAndCapturesCause() {
        AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> captured = new AtomicReference<>();
        AnalyticsSearchTransportService dispatcher = mockDispatcher(captured);

        ShardFragmentStageExecution exec = newExec(List.of(mockTarget("nodeA")), dispatcher);
        exec.start();

        RuntimeException cause = new RuntimeException("shard boom");
        captured.get().onFailure(cause);

        assertEquals(StageExecution.State.FAILED, exec.getState());
        Throwable captured_ex = exec.getFailure();
        assertNotNull(captured_ex);
        assertTrue(
            "stage failure wraps the shard failure",
            captured_ex.getMessage() != null && captured_ex.getMessage().contains("Stage 0 failed")
        );
        assertSame(cause, captured_ex.getCause());
        assertEquals("no batches fed on failure", 0, sink.batches.size());
    }

    public void testMultiShardWaitsForAllBeforeTransitioning() {
        List<AtomicReference<StreamingResponseListener<FragmentExecutionResponse>>> listeners = new ArrayList<>();
        AnalyticsSearchTransportService dispatcher = mockDispatcherPerInvocation(listeners);

        ShardFragmentStageExecution exec = newExec(
            List.of(mockTarget("nodeA"), mockTarget("nodeB"), mockTarget("nodeC")),
            dispatcher
        );
        exec.start();
        assertEquals(StageExecution.State.RUNNING, exec.getState());
        assertEquals("one listener per target", 3, listeners.size());

        // First two shards complete — stage still RUNNING.
        listeners.get(0).get().onStreamResponse(new FragmentExecutionResponse(newIntRoot("x", 1)), false);
        listeners.get(0).get().onStreamResponse(null, true);
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        listeners.get(1).get().onStreamResponse(null, true);
        assertEquals(StageExecution.State.RUNNING, exec.getState());

        // Last shard completes — stage transitions.
        listeners.get(2).get().onStreamResponse(new FragmentExecutionResponse(newIntRoot("x", 2)), false);
        listeners.get(2).get().onStreamResponse(null, true);
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
        assertEquals("only data batches fed (not completion signals)", 2, sink.batches.size());
    }

    public void testBatchesArrivingAfterCancelledAreDropped() {
        AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> captured = new AtomicReference<>();
        AnalyticsSearchTransportService dispatcher = mockDispatcher(captured);

        ShardFragmentStageExecution exec = newExec(List.of(mockTarget("nodeA")), dispatcher);
        exec.start();
        exec.cancel("test");
        assertEquals(StageExecution.State.CANCELLED, exec.getState());

        // Late arrivals after cancel are ignored (isDone() gate).
        VectorSchemaRoot lateBatch = newIntRoot("x", 9);
        try {
            captured.get().onStreamResponse(new FragmentExecutionResponse(lateBatch), false);
            assertEquals("late batch dropped after cancel", 0, sink.batches.size());
        } finally {
            lateBatch.close();  // sink didn't capture it, so we must release here
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private ShardFragmentStageExecution newExec(List<ShardTarget> targets, AnalyticsSearchTransportService dispatcher) {
        Stage stage = new Stage(0, null, List.of(), null);
        QueryContext config = new QueryContext(new QueryDAG("q-test", stage), Runnable::run, parentTask);
        FragmentExecutionRequest.PlanAlternative planAlt = new FragmentExecutionRequest.PlanAlternative("mock-backend", new byte[0]);
        return new ShardFragmentStageExecution(
            stage,
            config,
            sink,
            targets,
            target -> new FragmentExecutionRequest("q-test", 0, target.shardId(), List.of(planAlt)),
            dispatcher
        );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private AnalyticsSearchTransportService mockDispatcher(AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> out) {
        AnalyticsSearchTransportService d = mock(AnalyticsSearchTransportService.class);
        doAnswer(inv -> {
            out.set((StreamingResponseListener<FragmentExecutionResponse>) inv.getArgument(2));
            return null;
        }).when(d).dispatchFragment(any(FragmentExecutionRequest.class), any(DiscoveryNode.class), any(StreamingResponseListener.class), any(Task.class), any(PendingExecutions.class));
        return d;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private AnalyticsSearchTransportService mockDispatcherPerInvocation(
        List<AtomicReference<StreamingResponseListener<FragmentExecutionResponse>>> out
    ) {
        AnalyticsSearchTransportService d = mock(AnalyticsSearchTransportService.class);
        doAnswer(inv -> {
            AtomicReference<StreamingResponseListener<FragmentExecutionResponse>> ref = new AtomicReference<>(
                (StreamingResponseListener<FragmentExecutionResponse>) inv.getArgument(2)
            );
            out.add(ref);
            return null;
        }).when(d).dispatchFragment(any(FragmentExecutionRequest.class), any(DiscoveryNode.class), any(StreamingResponseListener.class), any(Task.class), any(PendingExecutions.class));
        return d;
    }

    private ShardTarget mockTarget(String nodeName) {
        DiscoveryNode node = new DiscoveryNode(
            nodeName,
            new TransportAddress(InetAddress.getLoopbackAddress(), 0),
            org.opensearch.Version.CURRENT
        );
        ShardId shardId = new ShardId(new Index("idx", UUID.randomUUID().toString()), 0);
        return new ShardTarget(shardId, node);
    }

    private VectorSchemaRoot newIntRoot(String fieldName, int rowCount) {
        Field field = new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector v = (IntVector) root.getVector(fieldName);
        v.allocateNew();
        for (int i = 0; i < rowCount; i++) v.setSafe(i, i);
        v.setValueCount(rowCount);
        root.setRowCount(rowCount);
        return root;
    }

    /** ExchangeSink that records fed batches for assertion. */
    private static final class RecordingSink implements ExchangeSink {
        final List<VectorSchemaRoot> batches = new ArrayList<>();

        @Override
        public void feed(VectorSchemaRoot batch) {
            batches.add(batch);
        }

        @Override
        public void close() {
            for (VectorSchemaRoot b : batches) b.close();
            batches.clear();
        }
    }
}
