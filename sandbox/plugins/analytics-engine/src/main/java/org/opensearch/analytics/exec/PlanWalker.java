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
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;

/**
 * Coordinator-side DAG recursion driver. One per query. Created by
 * {@code DefaultPlanExecutor}. Never blocks — takes an {@code ActionListener}
 * and signals completion via callbacks.
 *
 * <p>Pure recursion — no stage-type awareness. Gets the DAG from
 * {@link QueryContext} and mutates shared state via {@link QueryState}.
 * All per-stage-type dispatch decisions (LOCAL pass-through, LOCAL compute,
 * DATA_NODE fan-out) live in {@link StageExecutor}. This class simply
 * recurses through the DAG by passing {@code this::dispatchStage} as the
 * {@link ChildDispatcher} callback.
 *
 * @opensearch.internal
 */
public class PlanWalker {

    private static final Logger logger = LogManager.getLogger(PlanWalker.class);

    private final QueryContext config;
    private final QueryState state;
    private final StageExecutor stageExecutor;

    public PlanWalker(QueryContext config, QueryState state, StageExecutor stageExecutor) {
        this.config = config;
        this.state = state;
        this.stageExecutor = stageExecutor;
    }

    public String getQueryId() {
        return config.queryId();
    }

    /** Returns the coordinator-level query task for parent-child task propagation. */
    public Task getParentTask() {
        return config.parentTask();
    }

    public QueryContext getConfig() {
        return config;
    }

    public QueryState getState() {
        return state;
    }

    /**
     * Entry point. Walks the DAG bottom-up, feeding the root sink.
     * Calls the listener with the root sink's result when all stages complete.
     */
    public void walk(ShardRequestClient client, ActionListener<Iterable<Object[]>> listener) {
        dispatchStage(
            config.dag().rootStage(),
            state.rootSink(),
            client,
            ActionListener.wrap(v -> listener.onResponse(state.rootSink().readResult()), listener::onFailure)
        );
    }

    /**
     * Single delegation method. Delegates to {@link StageExecutor#dispatch}
     * with {@code this::dispatchStage} as the {@link ChildDispatcher} callback
     * for recursive child-stage walking.
     *
     * @param stage    the stage to dispatch
     * @param sink     the output sink for this stage's results
     * @param client   outbound shard client for transport dispatch
     * @param listener completion listener for this stage
     */
    public void dispatchStage(Stage stage, ExchangeSink sink, ShardRequestClient client, ActionListener<Void> listener) {
        stageExecutor.dispatch(stage, sink, client, this::dispatchStage, config, state, listener);
    }
}
