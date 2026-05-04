/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, input
 * schema (derived from the single child stage), and downstream sink. Hands
 * the resulting sink to {@link LocalStageExecution}.
 *
 * <p>Single-sink simplification: assumes exactly one child stage. Multi-child
 * (joins, set ops) will require per-child sink routing in a follow-up.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            chosenBytes(stage),
            config.bufferAllocator(),
            buildInputDescriptors(stage),
            sink
        );
        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, backendSink, context, sink);
    }

    /** Picks the plan-alternative bytes bound to the stage's exchange sink provider. */
    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    /**
     * Builds one {@link ExchangeSinkContext.InputDescriptor} per child stage. The
     * {@code inputId} convention is {@code "input-" + i} where {@code i} is the
     * child's index in {@link Stage#getChildStages()} — the same convention the
     * substrait fragment's {@code NamedScan} table names use, so registration
     * lines up by string equality.
     */
    private static List<ExchangeSinkContext.InputDescriptor> buildInputDescriptors(Stage stage) {
        List<Stage> children = stage.getChildStages();
        assert !children.isEmpty() : "COORDINATOR_REDUCE stage " + stage.getStageId() + " has no child stages";
        List<ExchangeSinkContext.InputDescriptor> result = new java.util.ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            Stage child = children.get(i);
            Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(child.getFragment().getRowType());
            result.add(new ExchangeSinkContext.InputDescriptor(child.getStageId(), "input-" + i, schema));
        }
        return result;
    }
}
