/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Coordinator-level plan executor. Registered as a {@link HandledTransportAction}
 * so that Guice injects all dependencies ({@link TransportService},
 * {@link ClusterService}, {@link ThreadPool}, etc.) automatically.
 *
 * <p>The SQL plugin resolves this class from the Node's Guice injector and invokes
 * {@link #execute(RelNode, Object)} directly. The transport path ({@code doExecute})
 * is reserved for future remote query invocation.
 *
 * @opensearch.internal
 */
public class DefaultPlanExecutor extends HandledTransportAction<ActionRequest, ActionResponse>
    implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    private final CapabilityRegistry capabilityRegistry;
    private final ClusterService clusterService;
    private final Scheduler scheduler;
    private final Executor searchExecutor;
    private final TaskManager taskManager;

    @Inject
    public DefaultPlanExecutor(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        CapabilityRegistry capabilityRegistry,
        EngineContext engineContext
    ) {
        super(AnalyticsQueryAction.NAME, transportService, actionFilters, in -> {
            throw new UnsupportedOperationException("Transport path not implemented yet");
        });
        this.capabilityRegistry = capabilityRegistry;
        this.clusterService = clusterService;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.taskManager = transportService.getTaskManager();
        this.scheduler = new Scheduler(transportService, 5);
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        QueryDAG dag = PlannerImpl.createPlan(logicalFragment, new PlannerContext(capabilityRegistry, clusterService.state()));
        logger.info("[DefaultPlanExecutor] QueryDAG:\n{}", dag);

        // Register coordinator-level query task with TaskManager (like SearchTask).
        // This gives us a proper unique ID, visibility in _tasks API, and cancellation support.
        Task queryTask = taskManager.register(
            "transport",
            "analytics_query",
            new AnalyticsQueryTaskRequest(dag.queryId())
        );

        PlanWalker walker = new PlanWalker(dag, clusterService, searchExecutor, queryTask);
        PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
        scheduler.execute(walker, ActionListener.wrap(result -> {
            taskManager.unregister(queryTask);
            future.onResponse(result);
        }, e -> {
            taskManager.unregister(queryTask);
            future.onFailure(e);
        }));
        return future.actionGet();  // TODO: single blocking point — will become async when API changes
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        // Transport path — reserved for future remote query invocation.
        // Currently, the SQL plugin invokes execute(RelNode, Object) directly.
        listener.onFailure(new UnsupportedOperationException("Direct invocation only — use execute(RelNode, Object)"));
    }

    /**
     * Lightweight {@link TaskAwareRequest} for registering an {@link AnalyticsQueryTask}
     * with {@link TaskManager}. Mirrors how {@code SearchRequest.createTask()} returns
     * a {@code SearchTask}.
     */
    static class AnalyticsQueryTaskRequest implements TaskAwareRequest {
        private final String queryId;
        private TaskId parentTaskId = TaskId.EMPTY_TASK_ID;

        AnalyticsQueryTaskRequest(String queryId) {
            this.queryId = queryId;
        }

        @Override
        public void setParentTask(TaskId taskId) {
            this.parentTaskId = taskId;
        }

        @Override
        public TaskId getParentTask() {
            return parentTaskId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new AnalyticsQueryTask(id, type, action, queryId, parentTaskId, headers);
        }
    }
}
