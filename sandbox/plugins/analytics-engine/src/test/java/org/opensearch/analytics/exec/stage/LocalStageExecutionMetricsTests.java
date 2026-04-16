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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that child stage rows flowing through per-child sinks are correctly
 * handled by the {@link LocalStageExecution} lifecycle.
 *
 * Validates: Requirements 3.2
 */
@SuppressWarnings("unchecked")
public class LocalStageExecutionMetricsTests extends OpenSearchTestCase {

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
     * Build a LOCAL compute stage with two mock child stages, each producing
     * 5 rows via a fake dispatch. Feed rows into the per-child sinks and
     * verify that the parent LocalStageExecution transitions to SUCCEEDED
     * after asyncFinalize completes.
     *
     * Validates: Requirements 3.2
     */
    public void testChildStageRowsFedIntoSinksBeforeStart() {
        int parentStageId = 0;
        int child0Id = 1;
        int child1Id = 2;
        int rowsPerChild = 5;

        // Build mock child stages
        Stage child0 = mock(Stage.class);
        when(child0.getStageId()).thenReturn(child0Id);
        when(child0.getChildStages()).thenReturn(Collections.emptyList());

        Stage child1 = mock(Stage.class);
        when(child1.getStageId()).thenReturn(child1Id);
        when(child1.getChildStages()).thenReturn(Collections.emptyList());

        // Build parent LOCAL stage with two children
        Stage parentStage = mock(Stage.class);
        when(parentStage.getStageId()).thenReturn(parentStageId);
        when(parentStage.getChildStages()).thenReturn(List.of(child0, child1));
        when(parentStage.getExecutionType()).thenReturn(StageExecutionType.LOCAL);

        // Create raw child sinks (RowProducingSink)
        SimpleExchangeSink rawChild0Sink = new SimpleExchangeSink();
        SimpleExchangeSink rawChild1Sink = new SimpleExchangeSink();

        // Mock LocalStageContext that returns raw sinks
        LocalStageContext ctx = mock(LocalStageContext.class);
        when(ctx.sinkFor(child0Id)).thenReturn(rawChild0Sink);
        when(ctx.sinkFor(child1Id)).thenReturn(rawChild1Sink);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onResponse(null);
            return null;
        }).when(ctx).asyncFinalize(any());

        // Build the parent stage's own metrics
        StageMetrics parentMetrics = new StageMetrics(parentStageId);

        // Construct LocalStageExecution
        LocalStageExecution exec = new LocalStageExecution(parentStage, ctx);

        // Simulate child dispatch: each child produces rowsPerChild rows.
        // Feed rows into the raw child sinks BEFORE start() — in the new
        // lifecycle, start() is the single "all inputs are ready, drain now" call.
        feedRows(rawChild0Sink, rowsPerChild);
        feedRows(rawChild1Sink, rowsPerChild);

        // Start the parent stage — transitions CREATED → RUNNING → SUCCEEDED
        exec.start();

        // Assert child sinks received the rows
        assertEquals("child0 sink rowCount must equal " + rowsPerChild, rowsPerChild, rawChild0Sink.getRowCount());
        assertEquals("child1 sink rowCount must equal " + rowsPerChild, rowsPerChild, rawChild1Sink.getRowCount());

        // Parent stage should have succeeded
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());

        // Clean up sinks
        rawChild0Sink.close();
        rawChild1Sink.close();
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private void feedRows(ExchangeSink sink, int rowCount) {
        List<Field> fields = List.of(new Field("col_a", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        Schema schema = new Schema(fields);
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        vsr.allocateNew();
        VarCharVector vec = (VarCharVector) vsr.getVector(0);
        for (int i = 0; i < rowCount; i++) {
            vec.setSafe(i, ("value_" + i).getBytes(StandardCharsets.UTF_8));
        }
        vec.setValueCount(rowCount);
        vsr.setRowCount(rowCount);
        sink.feed(vsr);
    }
}
