/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.canmatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.exec.stage.coordinator.LocalStageTask;
import org.opensearch.analytics.exec.stage.coordinator.LocalTaskRunner;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.List;

/**
 * Coordinator-side pre-filter stage: resolves all candidate shard targets, runs a
 * parallel can-match probe via {@link CanMatchPreFilterPhase}, publishes the
 * surviving subset as stage metadata so the parent {@code ShardFragmentStageExecution}
 * can build tasks only for the targets that could actually contain matching rows.
 *
 * <p>Single task per stage — the fan-out is internal to {@code CanMatchPreFilterPhase}.
 * Lifecycle: state→RUNNING on dispatch; task fires {@code listener.onResponse(null)}
 * when the phase completes (success or fail-open). On task SUCCESS the cascade in
 * {@link org.opensearch.analytics.exec.stage.StageExecution#attachChildren} surfaces
 * {@link #publishedMetadata()} to the parent before scheduling.
 *
 * <p>Failure semantics: fail-open at every level. Filter serialization error,
 * transport exception, no extractable filters, unknown backend, target resolution
 * failure — the published manifest is the full unfiltered list. Pruning is
 * best-effort; correctness must never depend on it.
 *
 * @opensearch.internal
 */
public final class CanMatchStage extends AbstractStageExecution {

    private static final Logger LOGGER = LogManager.getLogger(CanMatchStage.class);

    private final QueryContext config;
    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService dispatcher;
    private final List<CanMatchFilter> filters;
    private final String backendId;

    /**
     * Published to the parent via the cascade after the task completes. {@code null}
     * before completion; equal to the full target list on fail-open paths.
     */
    private volatile List<ExecutionTarget> publishedTargets;

    public CanMatchStage(
        Stage stage,
        QueryContext config,
        ClusterService clusterService,
        AnalyticsSearchTransportService dispatcher,
        List<CanMatchFilter> filters,
        String backendId
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.clusterService = clusterService;
        this.dispatcher = dispatcher;
        this.filters = filters;
        this.backendId = backendId;
        this.runner = new LocalTaskRunner(config.schedulerExecutor());
    }

    @Override
    public Object publishedMetadata() {
        return publishedTargets;
    }

    @Override
    protected List<StageTask> materializeTasks() {
        final List<ExecutionTarget> resolved;
        try {
            resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        } catch (Exception e) {
            // Target resolution failed — publish empty so parent sees "no work".
            // Parent will treat it the same as an empty resolve on its own path.
            publishedTargets = List.of();
            return List.of(noopTask());
        }
        if (resolved.isEmpty() || filters == null || filters.isEmpty() || backendId == null) {
            // Nothing to prune — publish the resolved list as-is, no transport round-trip.
            publishedTargets = resolved;
            return List.of(noopTask());
        }
        final byte[] filterBytes;
        try {
            filterBytes = CanMatchFilter.listToBytes(filters);
        } catch (IOException e) {
            LOGGER.warn("can-match filter serialization failed; failing open", e);
            publishedTargets = resolved;
            return List.of(noopTask());
        }
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(dispatcher.streamTransportService());
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), taskListener -> {
            phase.filter(resolved, filterBytes, backendId, new ActionListener<>() {
                @Override
                public void onResponse(List<ExecutionTarget> matching) {
                    publishedTargets = matching;
                    LOGGER.debug("can-match stage {} pruned {} → {} targets", getStageId(), resolved.size(), matching.size());
                    taskListener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    publishedTargets = resolved;
                    LOGGER.warn("can-match stage {} dispatch failed; failing open", getStageId(), e);
                    taskListener.onResponse(null);
                }
            });
        }));
    }

    /** Task that does nothing — used when the pre-filter is a no-op (empty resolve, no filters, etc.). */
    private LocalStageTask noopTask() {
        return new LocalStageTask(new StageTaskId(getStageId(), 0), taskListener -> taskListener.onResponse(null));
    }
}
