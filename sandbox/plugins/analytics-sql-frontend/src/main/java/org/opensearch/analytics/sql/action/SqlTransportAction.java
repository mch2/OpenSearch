/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action that coordinates SQL query execution.
 *
 * <p>Receives {@link EngineContextProvider} and {@link QueryPlanExecutor} from the analytics-engine
 * plugin via Guice injection (enabled by {@code extendedPlugins = ['analytics-engine']}).
 *
 * <p>Execution is forked to the {@link ThreadPool.Names#SEARCH} thread pool to avoid blocking the
 * transport thread ({@code DefaultPlanExecutor} uses a blocking future internally).
 */
public class SqlTransportAction extends HandledTransportAction<SqlRequest, SqlResponse> {

    private static final Logger logger = LogManager.getLogger(SqlTransportAction.class);

    private final SqlQueryService sqlQueryService;
    private final ThreadPool threadPool;

    @Inject
    public SqlTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ThreadPool threadPool
    ) {
        super(SqlExecuteAction.NAME, transportService, actionFilters, SqlRequest::new);
        this.sqlQueryService = new SqlQueryService(executor, contextProvider);
        this.threadPool = threadPool;
    }

    /** Test-only constructor that accepts a pre-built {@link SqlQueryService}. */
    public SqlTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SqlQueryService sqlQueryService,
        ThreadPool threadPool
    ) {
        super(SqlExecuteAction.NAME, transportService, actionFilters, SqlRequest::new);
        this.sqlQueryService = sqlQueryService;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, SqlRequest request, ActionListener<SqlResponse> listener) {
        // Fork to SEARCH thread pool — DefaultPlanExecutor.execute() blocks on a future internally,
        // which is forbidden on the transport thread.
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                SqlResponse response = request.isExplain()
                    ? sqlQueryService.executeWithProfile(request.getSql())
                    : sqlQueryService.execute(request.getSql());
                listener.onResponse(response);
            } catch (Exception e) {
                logger.error("[ANALYTICS_SQL] execution failed", e);
                listener.onFailure(e);
            }
        });
    }
}
