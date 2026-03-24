/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.plan.DefaultQueryPlanner;
import org.opensearch.analytics.plan.FieldCapabilityResolver;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.analytics.plan.registry.BackendCapabilityRegistry;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Coordinator-level plan executor. Plans the query and delegates shard-level
 * execution to {@link AnalyticsQueryService}.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final DefaultQueryPlanner queryPlanner;
    // TODO: - move out as data node side service
    private final AnalyticsQueryService queryService;

    @Inject
    public DefaultPlanExecutor(
        List<AnalyticsSearchBackendPlugin> plugins,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;

        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }

        // Build BackendCapabilityRegistry from plugins
        BackendCapabilityRegistry registry = new BackendCapabilityRegistry();
        for (AnalyticsSearchBackendPlugin plugin : plugins) {
            Set<Class<? extends RelNode>> ops = plugin.supportedOperators();
            Set<String> fns = extractFunctionNames(plugin);
            registry.register(plugin.name(), ops, fns, plugin);
        }

        // Build cluster for HepPlanner (used by DefaultQueryPlanner internally)
        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        FieldCapabilityResolver fieldCapabilityResolver =
            new FieldCapabilityResolver(indicesService, clusterService);

        this.queryPlanner = new DefaultQueryPlanner(registry, cluster, fieldCapabilityResolver);
        this.queryService = new AnalyticsQueryService(backEnds);
    }

    private static Set<String> extractFunctionNames(AnalyticsSearchBackendPlugin plugin) {
        if (plugin.operatorTable() == null) return Set.of();
        return plugin.operatorTable().getOperatorList().stream()
            .map(op -> op.getName().toUpperCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // --- Coordinator: plan ---
        String tableName = extractTableName(logicalFragment);
        IndexMetadata indexMetadata = clusterService.state().metadata().index(tableName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index [" + tableName + "] not found in cluster state");
        }
        int shardCount = indexMetadata.getNumberOfShards();

        ResolvedPlan plan = queryPlanner.plan(logicalFragment, shardCount);

        if ("unresolved".equals(plan.getPrimaryBackend())) {
            throw new IllegalStateException(
                "Planning did not resolve backend assignment for plan root");
        }

        logger.info("[DefaultPlanExecutor] Plan resolved to backend [{}]", plan.getPrimaryBackend());

        IndexShard shard = resolveShard(tableName);
        return queryService.execute(plan, shard);
    }

    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    private IndexShard resolveShard(String indexName) {
        IndexMetadata meta = clusterService.state().metadata().index(indexName);
        if (meta == null) throw new IllegalArgumentException("Index [" + indexName + "] not found");
        IndexService indexService = indicesService.indexService(meta.getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        IndexShard shard = indexService.getShardOrNull(shardIds.iterator().next());
        if (shard == null) throw new IllegalStateException("Shard not found");
        return shard;
    }
}
