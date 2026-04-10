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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
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
 * <p>Delegates the sliding-window dispatch pattern to {@link StageExec},
 * which owns all per-dispatch mutable state and the completion state machine.
 *
 * @opensearch.internal
 */
public class StageExecutor {

    private final String queryId;
    private final ClusterService clusterService;
    private final Executor searchExecutor;
    private final ExchangeSink rootSink;
    private final Set<Integer> completedStages = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();

    StageExecutor(String queryId, ClusterService clusterService, Executor searchExecutor, ExchangeSink rootSink) {
        this.queryId = queryId;
        this.clusterService = clusterService;
        this.searchExecutor = searchExecutor;
        this.rootSink = rootSink;
    }

    /**
     * Dispatches a single stage. Coordinator gather stages complete immediately.
     * All other stages delegate to {@link StageExec} for sliding-window
     * dispatch.
     */
    void dispatch(Stage stage, TaskSubmitter submitter, ActionListener<Void> listener) {
        // Coordinator gather — child results already in rootSink
        if (stage.isCoordinatorGather()) {
            completedStages.add(stage.getStageId());
            listener.onResponse(null);
            return;
        }

        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        List<TargetShard> targets = TargetResolver.resolveTargets(stage, clusterService, shuffleManifests);

        // ShardFilterPhase — always invoked, IDENTITY is no-op
        targets = stage.getShardFilterPhase().filter(targets, stage);

        // StageMetrics — record start
        StageMetrics metrics = new StageMetrics(stage.getStageId());
        metrics.recordStart();

        boolean collectMetadata = stage.isShuffleWrite();
        Map<ShardId, Map<Integer, String>> manifests = new ConcurrentHashMap<>();

        StageExec task = new StageExec(
            stage,
            targets,
            planAlternatives,
            collectMetadata,
            manifests,
            metrics,
            queryId,
            searchExecutor,
            rootSink,
            completedStages,
            shuffleManifests,
            submitter,
            listener
        );
        task.run();
    }

    private List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }
}
