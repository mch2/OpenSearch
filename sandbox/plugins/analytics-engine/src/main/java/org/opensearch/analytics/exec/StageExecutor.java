/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dispatches a single stage: resolves targets, applies shard filtering,
 * submits tasks via {@link TaskSubmitter}, collects responses, and feeds
 * the root sink. Extracted from {@link PlanWalker} for clarity.
 *
 * @opensearch.internal
 */
public class StageExecutor {
    private static final Logger logger = LogManager.getLogger(StageExecutor.class);

    private final String queryId;
    private final ClusterService clusterService;
    private final ExchangeSink rootSink;
    private final Map<Integer, PlanWalker.StageOutput> stageOutputs;
    private final Map<Integer, StageMetrics> stageMetrics;

    StageExecutor(
        String queryId,
        ClusterService clusterService,
        ExchangeSink rootSink,
        Map<Integer, PlanWalker.StageOutput> stageOutputs,
        Map<Integer, StageMetrics> stageMetrics
    ) {
        this.queryId = queryId;
        this.clusterService = clusterService;
        this.rootSink = rootSink;
        this.stageOutputs = stageOutputs;
        this.stageMetrics = stageMetrics;
    }

    /**
     * Dispatches a single stage. Coordinator gather stages complete immediately.
     * Row responses feed the sink; metadata responses are collected into a manifest map.
     */
    void dispatch(Stage stage, TaskSubmitter submitter, ActionListener<Void> listener) {
        // Coordinator gather — child results already in rootSink
        if (stage.isCoordinatorGather()) {
            stageOutputs.put(stage.getStageId(), new PlanWalker.StageOutput.RowData());
            listener.onResponse(null);
            return;
        }

        List<FragmentExecutionRequest.PlanAlternative> planAlternatives = buildPlanAlternatives(stage);
        List<PlanWalker.TargetShard> targets = TargetResolver.resolveTargets(stage, clusterService, stageOutputs);

        // ShardFilterPhase — always invoked, IDENTITY is no-op
        targets = stage.getShardFilterPhase().filter(targets, stage);

        // StageMetrics — record start
        StageMetrics metrics = new StageMetrics(stage.getStageId());
        metrics.recordStart();
        stageMetrics.put(stage.getStageId(), metrics);

        boolean collectMetadata = stage.isShuffleWrite();
        Map<ShardId, Map<Integer, String>> manifests = collectMetadata ? new ConcurrentHashMap<>() : null;

        // TerminationDecider — controls batch size (DISPATCH_ALL = all targets)
        TerminationDecider decider = stage.getTerminationDecider();
        int batchSize = decider.initialBatchSize(targets.size());

        AtomicInteger remaining = new AtomicInteger(targets.size());
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (PlanWalker.TargetShard target : targets) {
            FragmentExecutionRequest request = new FragmentExecutionRequest(
                queryId,
                stage.getStageId(),
                UUID.randomUUID().toString(),
                target.shardId(),
                planAlternatives
            );

            submitter.submit(request, target.node(), new ActionListener<>() {
                @Override
                public void onResponse(FragmentExecutionResponse response) {
                    if (response.hasMetadata()) {
                        manifests.put(target.shardId(), parseManifest(response.getMetadata()));
                    } else {
                        synchronized (rootSink) {
                            rootSink.feed(response);
                        }
                    }
                    metrics.incrementTasksCompleted();
                    checkComplete();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    metrics.incrementTasksFailed();
                    logger.error("Shard execution failed for stage {}: {}", stage.getStageId(), e.getMessage(), e);
                    checkComplete();
                }

                private void checkComplete() {
                    if (remaining.decrementAndGet() == 0) {
                        metrics.recordEnd();
                        Exception e = failure.get();
                        if (e != null) {
                            listener.onFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                        } else {
                            stageOutputs.put(
                                stage.getStageId(),
                                collectMetadata ? new PlanWalker.StageOutput.PartitionManifest(manifests) : new PlanWalker.StageOutput.RowData()
                            );
                            listener.onResponse(null);
                        }
                    }
                }
            });
        }
    }

    private List<FragmentExecutionRequest.PlanAlternative> buildPlanAlternatives(Stage stage) {
        List<FragmentExecutionRequest.PlanAlternative> alternatives = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            alternatives.add(new FragmentExecutionRequest.PlanAlternative(plan.backendId(), plan.convertedBytes()));
        }
        return alternatives;
    }

    private Map<Integer, String> parseManifest(Map<String, String> metadata) {
        Map<Integer, String> manifest = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            manifest.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return manifest;
    }
}
