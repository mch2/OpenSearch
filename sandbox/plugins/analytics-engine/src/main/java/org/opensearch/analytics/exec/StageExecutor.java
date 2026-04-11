/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Dispatches a single stage: resolves targets, applies shard filtering,
 * submits tasks via {@link TaskSubmitter}, collects responses, and feeds
 * the root sink. Response callbacks are forked to the search thread pool
 * to avoid blocking transport threads.
 *
 * <p>Delegates the sliding-window dispatch pattern to {@link StageExecution},
 * which owns all per-dispatch mutable state and the completion state machine.
 *
 * @opensearch.internal
 */
public class StageExecutor {

    private final ClusterService clusterService;
    private final QueryExecutionContext context;

    StageExecutor(
        String queryId,
        ClusterService clusterService,
        Executor searchExecutor,
        ExchangeSink rootSink,
        Task parentTask
    ) {
        this.clusterService = clusterService;
        Set<Integer> completedStages = ConcurrentHashMap.newKeySet();
        Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();
        this.context = new QueryExecutionContext(queryId, searchExecutor, rootSink, completedStages, shuffleManifests, parentTask);
    }

    /**
     * Dispatches a single stage. Coordinator gather stages complete immediately.
     * All other stages delegate to {@link StageExecution} for dispatch
     */
    void dispatch(Stage stage, TaskSubmitter submitter, ActionListener<Void> listener) {
        // Coordinator gather — child results already in rootSink
        if (stage.isCoordinatorGather()) {
            context.completedStages().add(stage.getStageId());
            listener.onResponse(null);
            return;
        }

        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        // Targets can be shards (fan-out) or intermediate nodes (determined by shuffle manifests)
        List<ShardTarget> targets = TargetResolver.resolveTargets(stage, clusterService, context.shuffleManifests());

        // ShardFilterPhase — always invoked, IDENTITY is no-op
        targets = stage.getShardFilterPhase().filter(targets, stage);

        new StageExecution(stage, targets, planAlternatives, context, submitter, listener).run();
    }

    private List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }
}
