/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link Stage} constructor's inferred-type heuristic.
 *
 * Validates: Requirements 1.3, 1.4
 */
public class StageTypeInferenceTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    private OpenSearchTableScan buildTableScan(String tableName) {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("default", tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, RelTraitSet.createEmpty(), table, List.of("lucene"), List.of());
    }

    /**
     * The constructor infers {@code LOCAL} when there is no exchange and no
     * table scan.
     *
     * Validates: Requirements 1.3
     */
    public void testNoExchangeNoTableScanInfersLocal() {
        org.apache.calcite.rel.RelNode fragment = mock(org.apache.calcite.rel.RelNode.class);
        when(fragment.getInputs()).thenReturn(List.of());

        Stage stage = new Stage(0, fragment, List.of(), null);

        assertEquals(
            "Constructor should infer LOCAL when no exchange and no table scan",
            StageExecutionType.LOCAL,
            stage.getExecutionType()
        );
    }

    /**
     * When a table scan is present, the constructor infers {@code DATA_NODE}.
     *
     * Validates: Requirements 1.4
     */
    public void testWithTableScanInfersDataNode() {
        OpenSearchTableScan scan = buildTableScan("test_table");

        Stage stage = new Stage(0, scan, List.of(), null);

        assertEquals(StageExecutionType.DATA_NODE, stage.getExecutionType());
    }

    /**
     * When an explicit execution type is provided, the constructor uses it
     * regardless of the fragment structure.
     */
    public void testExplicitExecutionTypeOverridesInference() {
        OpenSearchTableScan scan = buildTableScan("test_table");

        Stage stage = new Stage(0, scan, List.of(), null, StageExecutionType.LOCAL);

        assertEquals("Explicit LOCAL should override the inferred DATA_NODE", StageExecutionType.LOCAL, stage.getExecutionType());
    }
}
