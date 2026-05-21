/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.stage.AbstractStageExecution;
import org.opensearch.analytics.exec.stage.DataProducer;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.exec.stage.StageTaskState;
import org.opensearch.analytics.exec.task.TaskRunner;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Leaf stage: dispatches fragment work to data-node shards via Arrow streaming,
 * one {@link StageTask} per resolved target. Transport owned by {@link ShardTaskRunner};
 * data-arrival behavior by {@link #responseListenerFor}.
 *
 * @opensearch.internal
 */
public class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;

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
        this.runner = new ShardTaskRunner(this, config, dispatcher, requestBuilder);
    }

    @Override
    protected List<StageTask> materializeTasks() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        // Empty list → base short-circuits to SUCCEEDED (nothing to dispatch).
        List<StageTask> tasks = new ArrayList<>(resolved.size());
        for (int i = 0; i < resolved.size(); i++) {
            tasks.add(new ShardStageTask(new StageTaskId(getStageId(), i), resolved.get(i)));
        }
        return tasks;
    }

    /**
     * Incremental dispatch: emit only {@code maxConcurrentOutboundShards} tasks up front,
     * then dispatch the next one each time an in-flight task terminates. Bounds the depth
     * of {@link QueryContext#outboundShardThrottle()}'s internal queue to the dispatch
     * window — without this override, the eager default would enqueue {@code N - permits}
     * runnables behind the semaphore on large alias fan-outs.
     *
     * <p>The runAfter wrapping is per-slot: the scheduler-built listener runs first (drives
     * terminal state), then this wrapper advances the slot. This assumes a task's terminal is
     * final. {@link #retargetForRetry} is a no-op today; once it is wired, a retry re-dispatched
     * through the bare scheduler listener would slip outside this accounting — the failed task's
     * terminal advances the window while the retry runs unslotted, and the retry's own terminal
     * never re-advances — so the window logic must be revisited then.
     */
    @Override
    public void dispatchTasks(BiFunction<StageExecution, StageTask, ActionListener<Void>> handleFor) {
        @SuppressWarnings("unchecked")
        TaskRunner<StageTask> runner = (TaskRunner<StageTask>) taskRunner();
        List<StageTask> tasks = tasks();
        AtomicInteger nextIndex = new AtomicInteger(0);
        Runnable dispatchOne = new Runnable() {
            @Override
            public void run() {
                if (getState().isTerminal()) return;
                int idx = nextIndex.getAndIncrement();
                if (idx >= tasks.size()) return;
                StageTask task = tasks.get(idx);
                task.transitionTo(StageTaskState.RUNNING);
                ActionListener<Void> wrapped = ActionListener.runAfter(handleFor.apply(ShardFragmentStageExecution.this, task), this);
                runner.run(task, wrapped);
            }
        };
        int initialWindow = Math.min(tasks.size(), config.maxConcurrentOutboundShards());
        for (int i = 0; i < initialWindow; i++) {
            dispatchOne.run();
        }
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
