/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action that coordinates PPL query execution through the analytics engine.
 *
 * <p>Matches the SQL plugin's {@code RestUnifiedQueryAction} pattern:
 * PPL text → {@link UnifiedQueryPlanner} → {@link RelNode} → {@link QueryPlanExecutor} → rows.
 *
 * <p>No PushDownPlanner, no BoundaryTableScan, no Calcite JDBC/Janino.
 * The full RelNode (including LogicalAggregate) is passed directly to the
 * analytics engine's DefaultPlanExecutor for distributed execution.
 *
 * @opensearch.internal
 */
public class TestPPLTransportAction extends HandledTransportAction<PPLRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(TestPPLTransportAction.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final EngineContext engineContext;
    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> executor;

    @Inject
    public TestPPLTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.engineContext = engineContext;
        this.executor = executor;
    }

    @Override
    protected void doExecute(Task task, PPLRequest request, ActionListener<PPLResponse> listener) {
        try {
            // Parse PPL → RelNode (same as SQL plugin's UnifiedQueryPlanner)
            UnifiedQueryContext context = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog(DEFAULT_CATALOG, engineContext.getSchema())
                .defaultNamespace(DEFAULT_CATALOG)
                .build();

            RelNode plan;
            try {
                UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                plan = planner.plan(request.getPplText());
            } finally {
                try {
                    context.close();
                } catch (Exception ignored) {}
            }

            logger.info("[UNIFIED_PPL] RelNode plan:\n{}", org.apache.calcite.plan.RelOptUtil.toString(plan));

            // Execute directly through analytics engine (same as SQL plugin's AnalyticsExecutionEngine)
            Iterable<Object[]> rows = executor.execute(plan, null);

            // Build response from plan's row type + result rows
            List<String> columns = new ArrayList<>();
            for (RelDataTypeField field : plan.getRowType().getFieldList()) {
                columns.add(field.getName());
            }

            List<Object[]> rowList = new ArrayList<>();
            for (Object[] row : rows) {
                rowList.add(row);
            }

            listener.onResponse(new PPLResponse(columns, rowList));
        } catch (Exception e) {
            logger.error("[UNIFIED_PPL] execution failed", e);
            listener.onFailure(e);
        }
    }
}
