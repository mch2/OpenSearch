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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Stage} computed properties: tableName, isCoordinatorGather, isShuffleWrite.
 * These properties are computed once during Stage construction from the fragment.
 */
public class PlanWalkerHelpersTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName, List<String> viableBackends) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, viableBackends, List.of());
    }

    public void testTableNameFromTableScan() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        assertEquals("http_logs", stage.getTableName());
    }

    public void testTableNameNullForStageInputScan() {
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage stage = new Stage(0, stageInput, List.of(), null);
        assertNull(stage.getTableName());
    }

    public void testIsCoordinatorGatherNoExchangeNoTableScan() {
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(cluster, RelTraitSet.createEmpty(), 0, rowType, List.of());
        Stage stage = new Stage(1, stageInput, List.of(), null);
        assertTrue(stage.isCoordinatorGather());
    }

    public void testIsNotCoordinatorGatherWithTableScan() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        assertFalse(stage.isCoordinatorGather());
    }

    public void testIsNotCoordinatorGatherWithExchange() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage stage = new Stage(0, scan, List.of(), exchange);
        assertFalse(stage.isCoordinatorGather());
    }

    public void testIsShuffleWriteWithHashExchange() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, ShuffleImpl.FILE, List.of(0));
        Stage stage = new Stage(0, scan, List.of(), exchange);
        assertTrue(stage.isShuffleWrite());
    }

    public void testIsNotShuffleWriteWithSingletonExchange() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        ExchangeInfo exchange = new ExchangeInfo(RelDistribution.Type.SINGLETON, null, List.of());
        Stage stage = new Stage(0, scan, List.of(), exchange);
        assertFalse(stage.isShuffleWrite());
    }

    public void testIsNotShuffleWriteWithNoExchange() {
        OpenSearchTableScan scan = buildTableScan("http_logs", List.of("lucene"));
        Stage stage = new Stage(0, scan, List.of(), null);
        assertFalse(stage.isShuffleWrite());
    }
}
