/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.exec.AnalyticsQueryAction;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.analytics.exec.AnalyticsShardAction;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.TransportAnalyticsShardAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    /**
     * Creates a new analytics engine hub plugin.
     */
    public AnalyticsPlugin() {}

    private final List<AnalyticsSearchBackendPlugin> backEnds = new ArrayList<>();
    private final List<SearchBackEndPlugin<?>> storageBackends = new ArrayList<>();
    private SqlOperatorTable operatorTable;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<AnalyticsSearchBackendPlugin> loadedBackEnds = loader.loadExtensions(AnalyticsSearchBackendPlugin.class);
        List<?> loadedStorageBackends = loader.loadExtensions(SearchBackEndPlugin.class);
        logger.info("[AnalyticsPlugin] loadExtensions called: backEnds={}, storageBackends={}", loadedBackEnds, loadedStorageBackends);
        backEnds.addAll(loadedBackEnds);
        storageBackends.addAll((List) loadedStorageBackends);
        operatorTable = aggregateOperatorTables();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        DefaultEngineContext ctx = new DefaultEngineContext(clusterService, operatorTable);

        // Build scan format index and field storage factory from storage backends
        Map<String, List<String>> scanFormats = new LinkedHashMap<>();
        for (var sb : storageBackends) {
            for (var format : sb.getSupportedFormats()) {
                scanFormats.computeIfAbsent(format.name(), k -> new ArrayList<>()).add(sb.name());
            }
        }
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = (indexMetadata) -> new FieldStorageResolver(
            indexMetadata,
            storageBackends
        );

        CapabilityRegistry capabilityRegistry = new CapabilityRegistry(backEnds, fieldStorageFactory, scanFormats);

        // Build backends map for AnalyticsSearchService
        Map<String, AnalyticsSearchBackendPlugin> backEndsMap = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin be : backEnds) {
            backEndsMap.put(be.name(), be);
        }
        AnalyticsSearchService searchService = new AnalyticsSearchService(backEndsMap);

        // CapabilityRegistry and EngineContext are returned as components so Guice can
        // inject them into DefaultPlanExecutor (which is a HandledTransportAction
        // registered via getActions() — Guice constructs it after createComponents).
        return List.of(searchService, ctx, capabilityRegistry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).to(DefaultPlanExecutor.class);
            b.bind(EngineContext.class).to(DefaultEngineContext.class);
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(AnalyticsShardAction.INSTANCE, TransportAnalyticsShardAction.class),
            new ActionHandler<>(AnalyticsQueryAction.INSTANCE, DefaultPlanExecutor.class)
        );
    }

    private SqlOperatorTable aggregateOperatorTables() {
        // TODO: re-wire once operatorTable() is added back to AnalyticsSearchBackendPlugin
        return SqlOperatorTables.of();
    }

    /**
     * Default implementation of {@link EngineContext}.
     */
    record DefaultEngineContext(ClusterService clusterService, SqlOperatorTable operatorTable) implements EngineContext {

        @Override
        public SchemaPlus getSchema() {
            return OpenSearchSchemaBuilder.buildSchema(clusterService.state());
        }
    }

}
