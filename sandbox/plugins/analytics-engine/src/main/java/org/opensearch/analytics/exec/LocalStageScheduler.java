/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.core.action.ActionListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link StageScheduler} implementation for LOCAL stages. Handles both
 * pass-through (bare {@link OpenSearchStageInputScan} fragment) and compute
 * LOCAL stages (backend-driven via {@link AnalyticsSearchBackendPlugin}).
 *
 * <p>Extracted verbatim from {@code StageExecutor.dispatchLocalStage}.
 * The only changes are:
 * <ul>
 *   <li>{@code this.primaryBackend} instead of {@code StageExecutor.this.primaryBackend}</li>
 *   <li>{@link StageSchedulerHelpers#walkChildrenWithSink} for the pass-through path</li>
 *   <li>Helper methods ({@code pickPlanForPrimaryBackend}, {@code isPassThrough},
 *       {@code buildChildSchemas}) are private/package-private methods on this class</li>
 * </ul>
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    private static final Logger logger = LogManager.getLogger(LocalStageScheduler.class);

    private final AnalyticsSearchBackendPlugin primaryBackend;

    LocalStageScheduler(AnalyticsSearchBackendPlugin primaryBackend) {
        this.primaryBackend = primaryBackend;
    }

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
        if (isPassThrough(stage)) {
            logger.info("[LocalStage] pass-through stageId={} — bypassing backend", stage.getStageId());
            state.completedStages().add(stage.getStageId());
            if (stage.getChildStages().isEmpty()) {
                listener.onResponse(null);
                return;
            }
            StageSchedulerHelpers.walkChildrenWithSink(stage.getChildStages(), outputSink, client, childDispatcher, listener);
            return;
        }

        // Compute LOCAL
        if (primaryBackend == null) {
            listener.onFailure(
                new IllegalStateException(
                    "StageExecutor: no primaryBackend injected — cannot dispatch compute LOCAL stage "
                        + "(stageId="
                        + stage.getStageId()
                        + "). Use the 2-arg constructor with a backend."
                )
            );
            return;
        }

        StagePlan chosenPlan = pickPlanForPrimaryBackend(stage, listener);
        if (chosenPlan == null) return;   // error already signaled

        Map<Integer, Schema> childSchemas = buildChildSchemas(stage);

        LocalStageRequest req = new LocalStageRequest(
            config.queryId(),
            stage.getStageId(),
            chosenPlan.convertedBytes(),
            state.bufferAllocator(),
            outputSink,
            childSchemas
        );

        LocalStageContext ctx;
        try {
            ctx = primaryBackend.createLocalStage(req);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        StageMetrics parentMetrics = state.metricsFor(stage.getStageId());
        LocalStageExecution exec = new LocalStageExecution(stage, ctx, ActionListener.wrap(v -> {
            state.unregisterStageExecution(stage.getStageId());
            state.completedStages().add(stage.getStageId());
            listener.onResponse(null);
        }, e -> {
            state.unregisterStageExecution(stage.getStageId());
            listener.onFailure(e);
        }), parentMetrics);
        state.registerStageExecution(exec);
        exec.start();

        if (stage.getChildStages().isEmpty()) {
            exec.finalizeStage();
            return;
        }

        AtomicInteger remaining = new AtomicInteger(stage.getChildStages().size());
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (Stage child : stage.getChildStages()) {
            ExchangeSink rawChildSink = ctx.sinkFor(child.getStageId());
            StageMetrics childMetrics = state.metricsFor(child.getStageId());
            ExchangeSink childSink = new MetricsInstrumentedSink(childMetrics, rawChildSink);
            childDispatcher.dispatch(child, childSink, client, ActionListener.wrap(v -> {
                if (remaining.decrementAndGet() == 0) {
                    Exception e = failure.get();
                    if (e != null) {
                        exec.failChildStage(e);
                    } else {
                        exec.finalizeStage();
                    }
                }
            }, e -> {
                failure.compareAndSet(null, e);
                if (remaining.decrementAndGet() == 0) {
                    exec.failChildStage(failure.get());
                }
            }));
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private StagePlan pickPlanForPrimaryBackend(Stage stage, ActionListener<Void> listener) {
        if (primaryBackend == null) {
            listener.onFailure(
                new IllegalStateException(
                    "StageExecutor: no primaryBackend injected — cannot pick plan for compute LOCAL stage "
                        + "(stageId="
                        + stage.getStageId()
                        + ")"
                )
            );
            return null;
        }
        String backendName = primaryBackend.name();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            if (backendName.equals(plan.backendId())) {
                return plan;
            }
        }
        listener.onFailure(
            new IllegalStateException(
                "No StagePlan alternative for primary backend '"
                    + backendName
                    + "' on stageId="
                    + stage.getStageId()
                    + " (available: "
                    + stage.getPlanAlternatives().stream().map(StagePlan::backendId).toList()
                    + ")"
            )
        );
        return null;
    }

    private static boolean isPassThrough(Stage stage) {
        return stage.getFragment() == null || stage.getFragment() instanceof OpenSearchStageInputScan;
    }

    /**
     * Builds a map of child stage id → Arrow {@link Schema} from each child
     * stage's fragment row type. Used to construct
     * {@link LocalStageRequest}.
     */
    static Map<Integer, Schema> buildChildSchemas(Stage stage) {
        Map<Integer, Schema> childSchemas = new HashMap<>();
        for (Stage child : stage.getChildStages()) {
            childSchemas.put(child.getStageId(), ArrowSchemaFromCalcite.arrowSchemaFromRowType(child.getFragment().getRowType()));
        }
        return childSchemas;
    }
}
