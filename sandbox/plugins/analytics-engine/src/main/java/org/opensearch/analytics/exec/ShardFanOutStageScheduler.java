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
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link StageScheduler} implementation for DATA_NODE stages. Walks children
 * first via {@link StageSchedulerHelpers#walkChildrenWithSink}, then fans out
 * shard requests via {@link FanOutStageExecution}.
 *
 * <p>Extracted verbatim from {@code StageExecutor.dispatchDataNodeStage} +
 * {@code StageExecutor.fanOutDispatch} + {@code StageExecutor.buildPlanAlternatives}.
 *
 * <p>Handler selection on {@code stage.isShuffleWrite()} remains here as the
 * current behavior. {@code shuffle-exchange-foundation} later replaces this
 * path by adding a dedicated {@code ShuffleWriteStageScheduler} branch in the
 * router so this scheduler sees only non-shuffle DATA_NODE stages.
 *
 * @opensearch.internal
 */
final class ShardFanOutStageScheduler implements StageScheduler {

    private final ClusterService clusterService;

    ShardFanOutStageScheduler(ClusterService clusterService) {
        this.clusterService = clusterService;
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
        StageSchedulerHelpers.walkChildrenWithSink(
            stage.getChildStages(),
            outputSink,
            client,
            childDispatcher,
            ActionListener.wrap(v -> fanOutDispatch(stage, config, state, outputSink, client, listener), listener::onFailure)
        );
    }

    private void fanOutDispatch(
        Stage stage,
        QueryContext config,
        QueryState state,
        ExchangeSink outputSink,
        ShardRequestClient client,
        ActionListener<Void> listener
    ) {
        StageMetrics metrics = state.metricsFor(stage.getStageId());
        StageResultHandler handler;
        if (stage.isShuffleWrite()) {
            handler = new ManifestCollectingHandler();
        } else {
            ExchangeSink instrumentedSink = new MetricsInstrumentedSink(metrics, outputSink);
            handler = new SinkFeedingHandler(instrumentedSink);
        }

        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        List<ShardTarget> targets = TargetResolver.resolveTargets(stage, clusterService, state.shuffleManifests());
        targets = stage.getShardFilterPhase().filter(targets, stage);

        FanOutStageExecution exec = new FanOutStageExecution(
            stage,
            config.queryId(),
            targets,
            planAlternatives,
            config.searchExecutor(),
            config.parentTask(),
            state.rootSink(),
            handler,
            state.completedStages(),
            state.shuffleManifests(),
            client,
            ActionListener.wrap(v -> {
                state.unregisterStageExecution(stage.getStageId());
                listener.onResponse(null);
            }, e -> {
                state.unregisterStageExecution(stage.getStageId());
                listener.onFailure(e);
            }),
            metrics
        );

        state.registerStageExecution(exec);
        exec.run();
    }

    private static List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }
}
