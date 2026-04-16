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
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that child stage rows flowing through instrumented per-child sinks
 * are correctly counted in each child's {@link StageMetrics}.
 *
 * Validates: Requirements 3.2
 */
@SuppressWarnings("unchecked")
public class LocalStageExecutionMetricsTests extends OpenSearchTestCase {

    /**
     * Build a LOCAL compute stage with two mock child stages, each producing
     * 5 rows via a fake dispatch. Wrap each child sink with
     * {@link MetricsInstrumentedSink} and verify that each child's metrics
     * reflect the 5 rows produced.
     *
     * Validates: Requirements 3.2
     */
    public void testChildStageRowsCountedViaInstrumentedChildSink() {
        int parentStageId = 0;
        int child0Id = 1;
        int child1Id = 2;
        int rowsPerChild = 5;

        QueryState state = new QueryState();

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

        // Create raw child sinks (SimpleExchangeSink)
        SimpleExchangeSink rawChild0Sink = new SimpleExchangeSink();
        SimpleExchangeSink rawChild1Sink = new SimpleExchangeSink();

        // Wrap each child sink with MetricsInstrumentedSink using state.metricsFor
        StageMetrics child0Metrics = state.metricsFor(child0Id);
        StageMetrics child1Metrics = state.metricsFor(child1Id);
        ExchangeSink instrumentedChild0Sink = new MetricsInstrumentedSink(child0Metrics, rawChild0Sink);
        ExchangeSink instrumentedChild1Sink = new MetricsInstrumentedSink(child1Metrics, rawChild1Sink);

        // Mock LocalStageContext that returns instrumented sinks
        LocalStageContext ctx = mock(LocalStageContext.class);
        when(ctx.sinkFor(child0Id)).thenReturn(instrumentedChild0Sink);
        when(ctx.sinkFor(child1Id)).thenReturn(instrumentedChild1Sink);
        doAnswer(invocation -> {
            ActionListener<Void> finalizeListener = invocation.getArgument(0);
            finalizeListener.onResponse(null);
            return null;
        }).when(ctx).asyncFinalize(any());

        // Build the parent stage's own metrics
        StageMetrics parentMetrics = state.metricsFor(parentStageId);

        // Construct LocalStageExecution with externally-supplied metrics
        ActionListener<Void> stageListener = mock(ActionListener.class);
        LocalStageExecution exec = new LocalStageExecution(parentStage, ctx, stageListener, parentMetrics);
        exec.start();

        // Simulate child dispatch: each child produces rowsPerChild rows
        // Feed rows into the instrumented child sinks (simulating what childDispatcher does)
        feedRows(instrumentedChild0Sink, rowsPerChild);
        feedRows(instrumentedChild1Sink, rowsPerChild);

        // Finalize the parent stage
        exec.finalizeStage();

        // Assert child metrics
        assertEquals("child0 rowsProcessed must equal " + rowsPerChild, rowsPerChild, state.metricsFor(child0Id).getRowsProcessed());
        assertEquals("child1 rowsProcessed must equal " + rowsPerChild, rowsPerChild, state.metricsFor(child1Id).getRowsProcessed());

        // Parent stage should have succeeded
        assertEquals(StageExecution.State.SUCCEEDED, exec.getState());
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private void feedRows(ExchangeSink sink, int rowCount) {
        List<String> fieldNames = List.of("col_a");
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(new Object[] { "value_" + i });
        }
        FragmentExecutionResponse response = new FragmentExecutionResponse(fieldNames, rows);
        sink.feed(response);
    }
}
