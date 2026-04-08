/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-side DAG traversal. One per query. Created by
 * {@code DefaultPlanExecutor}, passed to {@code Scheduler}. Never blocks —
 * takes an {@code ActionListener} and signals completion via callbacks.
 *
 * <p>Owns the root sink and stage outputs map. Delegates stage dispatch to
 * {@link StageExecutor} and target resolution to {@link TargetResolver}.
 *
 * @opensearch.internal
 */
public class PlanWalker {

    // Immutable query context
    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final Task parentTask;
    private final StageExecutor stageExecutor;

    // Per-query mutable state
    private final ExchangeSink rootSink;
    private final Map<Integer, StageOutput> stageOutputs = new HashMap<>();
    private final Map<Integer, StageMetrics> stageMetrics = new HashMap<>();

    /** Output produced by a completed stage. */
    public sealed interface StageOutput {
        /** Rows already fed into the root sink. No additional data. */
        record RowData() implements StageOutput {}

        /** Partition manifests from each shard: shardId → (partitionId → filePath). */
        record PartitionManifest(Map<ShardId, Map<Integer, String>> manifests) implements StageOutput {}
    }

    /** Shard + node pairing. */
    public record TargetShard(ShardId shardId, DiscoveryNode node) {}

    public PlanWalker(QueryDAG dag, ClusterService clusterService, Executor searchExecutor, Task parentTask) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.rootSink = createRootSink(dag.rootStage());
        this.stageExecutor = new StageExecutor(dag.queryId(), clusterService, rootSink, stageOutputs, stageMetrics);
    }

    public String getQueryId() {
        return dag.queryId();
    }

    /** Returns the coordinator-level query task for parent-child task propagation. */
    public Task getParentTask() {
        return parentTask;
    }

    /** Fork a runnable to the search thread pool. */
    void fork(Runnable runnable) {
        searchExecutor.execute(runnable);
    }

    /**
     * Walks the DAG bottom-up, dispatching tasks for each stage asynchronously.
     * Calls the listener with the root sink's result when all stages complete.
     */
    public void walk(TaskSubmitter submitter, ActionListener<Iterable<Object[]>> listener) {
        walkStage(dag.rootStage(), submitter, ActionListener.wrap(v -> listener.onResponse(rootSink.readResult()), listener::onFailure));
    }

    /**
     * Walks a single stage: first walks all children (parallel or sequential based
     * on the stage's parallelChildren flag), then dispatches this stage.
     */
    private void walkStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> stageListener) {
        ActionListener<Void> dispatchAfterChildren = ActionListener.wrap(
            v -> stageExecutor.dispatch(stage, submitter, stageListener),
            stageListener::onFailure
        );
        if (stage.isParallelChildren()) {
            walkChildrenInParallel(stage.getChildStages(), submitter, dispatchAfterChildren);
        } else {
            walkChildrenSequentially(stage.getChildStages(), 0, submitter, dispatchAfterChildren);
        }
    }

    /**
     * Walks child stages one at a time, left to right. When all children are done,
     * calls the listener. Each child's completion triggers the next child.
     */
    private void walkChildrenSequentially(List<Stage> children, int index, TaskSubmitter submitter, ActionListener<Void> listener) {
        if (index >= children.size()) {
            listener.onResponse(null);
            return;
        }
        walkStage(
            children.get(index),
            submitter,
            ActionListener.wrap(v -> walkChildrenSequentially(children, index + 1, submitter, listener), listener::onFailure)
        );
    }

    /**
     * Walks all child stages concurrently. Uses AtomicInteger for remaining count
     * and AtomicReference for first-failure capture, consistent with the dispatch pattern.
     * Completion is signaled only after all children finish (success or failure).
     */
    private void walkChildrenInParallel(List<Stage> children, TaskSubmitter submitter, ActionListener<Void> listener) {
        if (children.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        AtomicInteger remaining = new AtomicInteger(children.size());
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (Stage child : children) {
            walkStage(child, submitter, new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    if (remaining.decrementAndGet() == 0) {
                        Exception e = failure.get();
                        if (e != null) {
                            listener.onFailure(e);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    if (remaining.decrementAndGet() == 0) {
                        listener.onFailure(failure.get());
                    }
                }
            });
        }
    }

    /**
     * Creates the root sink from the root stage's fragment.
     * MVP: SimpleExchangeSink (no computation, just collects rows).
     * Future: backend-provided sink embedding the root stage's computation
     * (final aggregate, sort, filter, project) for streaming reduction.
     */
    ExchangeSink createRootSink(Stage rootStage) {
        return new SimpleExchangeSink();
    }
}
