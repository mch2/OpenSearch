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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-side DAG traversal. One per query. Created by
 * {@code DefaultPlanExecutor}, passed to {@code Scheduler}. Never blocks —
 * takes an {@code ActionListener} and signals completion via callbacks.
 *
 * <p>Owns the root sink. Delegates stage dispatch to {@link StageExecutor}
 * and target resolution to {@link TargetResolver}.
 *
 * @opensearch.internal
 */
public class PlanWalker {

    private final QueryDAG dag;
    private final Task parentTask;
    private final ExchangeSink rootSink;
    private final StageExecutor stageExecutor;

    public PlanWalker(QueryDAG dag, ClusterService clusterService, Executor searchExecutor, Task parentTask) {
        this.dag = dag;
        this.parentTask = parentTask;
        this.rootSink = createRootSink(dag.rootStage());
        this.stageExecutor = new StageExecutor(dag.queryId(), clusterService, searchExecutor, rootSink);
    }

    public String getQueryId() {
        return dag.queryId();
    }

    /** Returns the coordinator-level query task for parent-child task propagation. */
    public Task getParentTask() {
        return parentTask;
    }

    /**
     * Walks the DAG bottom-up, dispatching tasks for each stage asynchronously.
     * Calls the listener with the root sink's result when all stages complete.
     */
    public void walk(TaskSubmitter submitter, ActionListener<Iterable<Object[]>> listener) {
        walkStage(dag.rootStage(), submitter, ActionListener.wrap(v -> listener.onResponse(rootSink.readResult()), listener::onFailure));
    }

    /**
     * Walks a single stage: first walks all children concurrently,
     * then dispatches this stage after all children complete.
     */
    private void walkStage(Stage stage, TaskSubmitter submitter, ActionListener<Void> stageListener) {
        walkChildren(
            stage.getChildStages(),
            submitter,
            ActionListener.wrap(v -> stageExecutor.dispatch(stage, submitter, stageListener), stageListener::onFailure)
        );
    }

    /**
     * Walks all child stages concurrently. Uses AtomicInteger for remaining count
     * and AtomicReference for first-failure capture. Completion is signaled only
     * after all children finish (success or failure).
     */
    private void walkChildren(List<Stage> children, TaskSubmitter submitter, ActionListener<Void> listener) {
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
                    // don't move to the next stage until all children have completed
                    // Sink is still accumulating batches within the stage.
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
