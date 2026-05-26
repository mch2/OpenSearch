/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Leaf stage: dispatches fragment work to data-node shards via Arrow streaming,
 * one {@link StageTask} per resolved target. Transport owned by {@link ShardTaskRunner};
 * data-arrival behavior by {@link #responseListenerFor}.
 *
 * @opensearch.internal
 */
public class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private static final Logger LOGGER = LogManager.getLogger(ShardFragmentStageExecution.class);

    /** Bound on the synchronous wait for the can-match dispatch. Conservative — long enough
     *  to absorb a slow per-shard response, short enough that a stuck data node doesn't park
     *  the whole stage forever. On timeout we fail-open (return all targets). */
    private static final TimeValue CAN_MATCH_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService dispatcher;

    public ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.dispatcher = dispatcher;
        this.runner = new ShardTaskRunner(this, config, dispatcher, requestBuilder);
    }

    /**
     * Synchronous path is unused now ({@link #materializeTasksAsync} drives the lifecycle),
     * but the abstract template still requires the method. Implements the no-canMatch path
     * — direct target resolution + task build — kept for unit tests that exercise the base
     * template via mocks.
     */
    @Override
    protected List<StageTask> materializeTasks() {
        return buildTasks(stage.getTargetResolver().resolve(clusterService.state(), null));
    }

    /**
     * Async materialise: resolve targets, dispatch can-match concurrently to each target,
     * publish only the surviving subset.
     *
     * <p>Failure modes (timeout, transport error, no extractable filters, unknown backend)
     * fall back to the full target list — pruning is best-effort and must never produce
     * incorrect results.
     */
    @Override
    protected void materializeTasksAsync(ActionListener<List<StageTask>> listener) {
        final List<ExecutionTarget> resolved;
        try {
            resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (resolved.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }
        List<CanMatchFilter> filters = stage.getCanMatchFilters();
        String backendId = resolveBackendId();
        if (filters == null || filters.isEmpty() || backendId == null) {
            listener.onResponse(buildTasks(resolved));
            return;
        }
        final byte[] filterBytes;
        try {
            filterBytes = CanMatchFilter.listToBytes(filters);
        } catch (IOException e) {
            LOGGER.warn("can-match filter serialization failed; skipping prune", e);
            listener.onResponse(buildTasks(resolved));
            return;
        }
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(dispatcher.streamTransportService());
        phase.filter(resolved, filterBytes, backendId, new ActionListener<List<ExecutionTarget>>() {
            @Override
            public void onResponse(List<ExecutionTarget> matching) {
                LOGGER.debug("can-match pruned {} → {} targets", resolved.size(), matching.size());
                listener.onResponse(buildTasks(matching));
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.warn("can-match dispatch failed; falling back to all targets", e);
                listener.onResponse(buildTasks(resolved));
            }
        });
    }

    private List<StageTask> buildTasks(List<ExecutionTarget> targets) {
        List<StageTask> tasks = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            tasks.add(new ShardStageTask(new StageTaskId(getStageId(), i), targets.get(i)));
        }
        return tasks;
    }

    /**
     * Sync convenience used by {@code ShardFragmentStageExecutionCanMatchTests}; identical
     * semantics to the async path but blocks on a future. Kept for unit-test coverage of
     * every short-circuit branch.
     */
    static List<ExecutionTarget> applyCanMatchFilter(
        List<ExecutionTarget> targets,
        List<CanMatchFilter> filters,
        String backendId,
        CanMatchPreFilterPhase phase,
        TimeValue timeout
    ) {
        if (targets.isEmpty() || filters == null || filters.isEmpty() || backendId == null) {
            return targets;
        }
        byte[] filterBytes;
        try {
            filterBytes = CanMatchFilter.listToBytes(filters);
        } catch (IOException e) {
            LOGGER.warn("can-match filter serialization failed; skipping prune", e);
            return targets;
        }
        org.opensearch.action.support.PlainActionFuture<List<ExecutionTarget>> future = new org.opensearch.action.support.PlainActionFuture<>();
        try {
            phase.filter(targets, filterBytes, backendId, future);
            List<ExecutionTarget> matching = future.actionGet(timeout);
            LOGGER.debug("can-match pruned {} → {} targets", targets.size(), matching.size());
            return matching;
        } catch (Exception e) {
            LOGGER.warn("can-match dispatch failed; falling back to all targets", e);
            return targets;
        }
    }

    /** Pulls the backend id off the first plan alternative; {@code null} when none present. */
    private String resolveBackendId() {
        List<org.opensearch.analytics.planner.dag.StagePlan> plans = stage.getPlanAlternatives();
        if (plans == null || plans.isEmpty()) {
            return null;
        }
        return plans.get(0).backendId();
    }

    // TODO: override retargetForRetry for replica failover — needs TargetResolver.alternateReplica
    // and per-task attempt tracking. Scheduler-side wiring is already in place.
    //
    // FOLLOW-UP: per-stage cancel granularity. Today AbstractStageExecution.cancel cancels
    // the whole parent task (via ct.cancel) to terminate in-flight data-node Flight streams.
    // That's coarse — fine for current query shapes (one failure means the query fails) but
    // it masks the real failure cause as "TaskCancelledException" in QueryExecution.terminalCause,
    // and forecloses speculative-execution / per-stage abort. Surgical alternative: track
    // per-task child-task-ids in ShardTaskRunner; cancel just those when this stage's
    // onTerminalTransition fires CANCELLED.

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    /**
     * Runs inline on the per-stream virtual thread driving handleStreamResponse — must NOT
     * offload: reordering would let isLast race ahead and drop earlier batches via the
     * stage-terminal short-circuit. Inline also preserves end-to-end backpressure.
     */
    StreamingResponseListener<FragmentExecutionArrowResponse> responseListenerFor(ActionListener<Void> listener) {
        return new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                VectorSchemaRoot vsr = response.getRoot();
                if (getState().isTerminal()) {
                    if (vsr != null) vsr.close();
                    return;
                }
                if (vsr == null) {
                    if (isLast) listener.onResponse(null);
                    return;
                }
                try {
                    outputSink.feed(vsr);
                } catch (Exception e) {
                    // Sink didn't take ownership — close the VSR before surfacing.
                    RuntimeException wrapped = new RuntimeException("Stage " + getStageId() + " sink feed failed", e);
                    try {
                        vsr.close();
                    } catch (IllegalStateException closeFailure) {
                        wrapped.addSuppressed(closeFailure);
                    }
                    listener.onFailure(wrapped);
                    return;
                }
                metrics.addRowsProcessed(vsr.getRowCount());
                if (isLast) listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException("Stage " + getStageId() + " failed", e));
            }
        };
    }
}
