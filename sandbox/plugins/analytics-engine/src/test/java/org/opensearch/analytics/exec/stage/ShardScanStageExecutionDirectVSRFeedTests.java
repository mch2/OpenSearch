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
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
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
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link ShardScanStageExecution} feeds the {@link VectorSchemaRoot}
 * from {@link FragmentExecutionResponse#getRoot()} directly into the sink
 * without any intermediate conversion.
 *
 * <p>The old {@code scanResponseToArrow} method has been deleted — the compile
 * gate in task 11 verifies it no longer exists. This test validates the runtime
 * behaviour: the exact VSR instance from the response is the one fed to the sink.
 *
 * Validates: Requirements 1.9
 */
public class ShardScanStageExecutionDirectVSRFeedTests extends OpenSearchTestCase {

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

    /**
     * Dispatches one shard, simulates a streaming response carrying a real
     * {@link VectorSchemaRoot}, and asserts that {@code sink.feed()} receives
     * the exact same VSR instance — proving no conversion takes place.
     */
    public void testOnStreamResponseFeedsVSRDirectly() {
        int numTargets = 1;
        Stage stage = mockStage();
        List<ShardTarget> targets = buildTargets(numTargets);

        // Capturing sink that records every VSR fed into it
        CapturingSink sink = new CapturingSink();

        List<StreamingResponseListener<FragmentExecutionResponse>> captured = new ArrayList<>();
        AnalyticsSearchTransportService dispatcher = capturingDispatcher(captured);

        QueryContext config = QueryContext.forTest("test-query", null);

        ShardScanStageExecution exec = new ShardScanStageExecution(
            stage,
            config,
            sink,
            targets,
            requestFn(stage),
            dispatcher
        );

        exec.start();
        assertEquals("One target must produce one captured listener", 1, captured.size());

        // Build a real VSR with data
        Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        IntVector idVec = (IntVector) vsr.getVector("id");
        idVec.allocateNew(3);
        idVec.set(0, 10);
        idVec.set(1, 20);
        idVec.set(2, 30);
        idVec.setValueCount(3);
        vsr.setRowCount(3);

        FragmentExecutionResponse response = new FragmentExecutionResponse(vsr);

        // Simulate the streaming callback
        captured.get(0).onStreamResponse(response, true);

        // The sink must have received the exact same VSR instance — no conversion
        assertEquals("Sink must have received exactly one batch", 1, sink.batches.size());
        assertSame("sink.feed() must receive the same VSR instance from the response", vsr, sink.batches.get(0));

        // Metrics must reflect the row count
        assertEquals("rowsProcessed must equal VSR row count", 3, exec.getMetrics().getRowsProcessed());
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());

        vsr.close();
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private Stage mockStage() {
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

    private static AnalyticsSearchTransportService capturingDispatcher(
        List<StreamingResponseListener<FragmentExecutionResponse>> captured
    ) {
        return new AnalyticsSearchTransportService(mock(TransportService.class), mock(ClusterService.class)) {
            @Override
            public void dispatchFragment(
                FragmentExecutionRequest request,
                DiscoveryNode node,
                StreamingResponseListener<FragmentExecutionResponse> listener,
                Task parentTask,
                PendingExecutions pending
            ) {
                captured.add(listener);
            }
        };
    }

    /**
     * Minimal {@link ExchangeSink} that records every VSR fed into it,
     * allowing identity assertions on the exact instances received.
     */
    private static class CapturingSink implements ExchangeSink {
        final List<VectorSchemaRoot> batches = new ArrayList<>();

        @Override
        public void feed(VectorSchemaRoot batch) {
            batches.add(batch);
        }

        @Override
        public void close() {}

        @Override
        public Iterable<Object[]> readResult() {
            return List.of();
        }

        @Override
        public long getRowCount() {
            return batches.stream().mapToLong(VectorSchemaRoot::getRowCount).sum();
        }

        @Override
        public Object getValueAt(String column, int rowIndex) {
            return null;
        }
    }
}
