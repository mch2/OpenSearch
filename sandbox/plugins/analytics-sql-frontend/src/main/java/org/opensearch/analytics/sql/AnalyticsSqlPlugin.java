/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.sql.action.RestSqlQueryAction;
import org.opensearch.analytics.sql.action.SqlExecuteAction;
import org.opensearch.analytics.sql.action.SqlTransportAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * SQL front-end plugin for Analytics Engine.
 *
 * <p>Exposes {@code POST /_analytics/sql}, parses regular SQL directly with Calcite against AE's
 * schema/types/functions, and executes the resulting logical {@code RelNode} through AE's
 * {@code QueryPlanExecutor}. {@code EngineContextProvider} and {@code QueryPlanExecutor} are
 * received by {@link SqlTransportAction} via Guice injection (enabled by
 * {@code extendedPlugins = ['analytics-engine']}).
 */
public class AnalyticsSqlPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(SqlExecuteAction.INSTANCE, SqlTransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestSqlQueryAction());
    }
}
