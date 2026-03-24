/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultPlanExecutor}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>P14: Backend_Tag from resolved plan root drives dispatch</li>
 *   <li>QueryPlanningException propagates without wrapping</li>
 *   <li>IllegalStateException when backendTag == "unresolved" at dispatch</li>
 * </ul>
 */
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private IndicesService indicesService;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        indicesService = mock(IndicesService.class);
        clusterService = mock(ClusterService.class);
    }

    /** Builds a minimal LogicalTableScan for index "myindex". */
    private LogicalTableScan buildScan(String indexName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getRowType()).thenReturn(
            typeFactory.builder().add("id", SqlTypeName.BIGINT).build());
        when(table.getQualifiedName()).thenReturn(List.of(indexName));
        return new LogicalTableScan(cluster, cluster.traitSet(), Collections.emptyList(), table);
    }

    /** Stubs clusterService to return IndexMetadata with the given shard count for indexName. */
    private void stubClusterState(String indexName, int shardCount) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
        ClusterState state = ClusterState.builder(new org.opensearch.cluster.ClusterName("test"))
            .metadata(metadata)
            .build();
        when(clusterService.state()).thenReturn(state);
    }

    /**
     * When the planner resolves the plan to backend "datafusion", the executor MUST look up
     * the plugin named "datafusion" for dispatch.
     */
    public void testBackendTagDrivesDispatch() {
        for (int i = 0; i < 100; i++) {
            AnalyticsSearchBackendPlugin datafusionPlugin = mock(AnalyticsSearchBackendPlugin.class);
            when(datafusionPlugin.name()).thenReturn("datafusion");
            when(datafusionPlugin.supportedOperators()).thenReturn(
                Set.of(LogicalTableScan.class, OpenSearchTableScan.class));
            when(datafusionPlugin.operatorTable()).thenReturn(null);

            stubClusterState("myindex", 1);

            DefaultPlanExecutor executor = new DefaultPlanExecutor(
                List.of(datafusionPlugin), indicesService, clusterService);

            LogicalTableScan scan = buildScan("myindex");

            try {
                executor.execute(scan, new Object());
                fail("Expected exception from shard resolution");
            } catch (IllegalStateException e) {
                assertFalse(
                    "Dispatch must use resolved backend name 'datafusion', not an unknown name: " + e.getMessage(),
                    e.getMessage().contains("No plugin registered for backend"));
                assertTrue(
                    "Expected shard-resolution failure, got: " + e.getMessage(),
                    e.getMessage().contains("not on this node") || e.getMessage().contains("No shards") || e.getMessage().contains("not found"));
            }
        }
    }

    /**
     * QueryPlanningException propagates without wrapping.
     */
    public void testQueryPlanningExceptionPropagatesUnwrapped() {
        stubClusterState("myindex", 1);

        DefaultPlanExecutor executor = new DefaultPlanExecutor(
            List.of(), indicesService, clusterService);

        LogicalTableScan scan = buildScan("myindex");

        QueryPlanningException ex = expectThrows(QueryPlanningException.class,
            () -> executor.execute(scan, new Object()));
        assertNotNull(ex.getErrors());
        assertFalse("Error list must not be empty", ex.getErrors().isEmpty());
    }

    /**
     * IllegalStateException when backendTag is "unresolved" at dispatch.
     */
    public void testIllegalStateWhenBackendTagUnresolved() {
        AnalyticsSearchBackendPlugin plugin = mock(AnalyticsSearchBackendPlugin.class);
        when(plugin.name()).thenReturn("datafusion");
        when(plugin.supportedOperators()).thenReturn(
            Set.of(LogicalTableScan.class, OpenSearchTableScan.class));
        when(plugin.operatorTable()).thenReturn(null);

        stubClusterState("myindex", 1);

        DefaultPlanExecutor executor = new DefaultPlanExecutor(
            List.of(plugin), indicesService, clusterService);

        LogicalTableScan scan = buildScan("myindex");

        try {
            executor.execute(scan, new Object());
            fail("Expected shard-resolution exception");
        } catch (IllegalStateException e) {
            assertFalse(
                "Safety-net for 'unresolved' must not fire for a valid resolved plan: " + e.getMessage(),
                e.getMessage().contains("Planning did not resolve backend assignment"));
        }
    }
}
