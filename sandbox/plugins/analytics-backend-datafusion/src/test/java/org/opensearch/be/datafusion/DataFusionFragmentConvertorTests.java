/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;

/**
 * Tests for {@link DataFusionFragmentConvertor#convertLocalStageFragment}.
 * Verifies that local stage fragments produce Substrait plans with
 * correctly renamed stage input table references.
 *
 * <p>Validates: Requirements 7.1
 */
public class DataFusionFragmentConvertorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Given a fragment Aggregate(...) → StageInputScan with child stage IDs [0],
     * assert the generated Substrait plan has a ReadRel whose table name is __stage_0_input__.
     *
     * <p>The test builds a pre-rewritten fragment (StageInputTableScan instead of
     * OpenSearchStageInputScan) because the rewrite is done by FragmentConversionDriver
     * before calling the convertor. This tests the convertor's Substrait conversion
     * in isolation.
     */
    public void testConvertLocalStageFragmentRenamesStageInputScans() throws Exception {
        // Build row type: single nullable INTEGER column "A"
        RelDataType rowType = typeFactory.builder()
            .add("A", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
            .build();

        // StageInputTableScan leaf — simulates the rewritten StageInputScan with stage input ID
        RelNode stageInput = new FragmentConversionDriver.StageInputTableScan(cluster, cluster.traitSet(), "__stage_0_input__", rowType);

        // Aggregate: SUM(A) — scalar aggregate (no group keys)
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "sum_A"
        );
        RelNode aggregate = LogicalAggregate.create(stageInput, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        // Convert using the local stage path
        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor();
        byte[] bytes = convertor.convertLocalStageFragment(aggregate, List.of("__stage_0_input__"));

        assertNotNull("Converted bytes should not be null", bytes);
        assertTrue("Converted bytes should not be empty", bytes.length > 0);

        // Deserialize the Substrait proto plan and find the ReadRel
        Plan plan = Plan.parseFrom(bytes);
        assertFalse("Plan should have at least one relation", plan.getRelationsList().isEmpty());

        String tableName = findReadRelTableName(plan);
        assertEquals(
            "StageInputScan should be converted to a ReadRel with the stage input ID as table name",
            "__stage_0_input__",
            tableName
        );
    }

    /**
     * Walks the Substrait proto plan to find the first ReadRel's named_table name.
     */
    private String findReadRelTableName(Plan plan) {
        for (PlanRel planRel : plan.getRelationsList()) {
            if (planRel.hasRoot()) {
                RelRoot root = planRel.getRoot();
                return findReadRelInRel(root.getInput());
            }
            if (planRel.hasRel()) {
                return findReadRelInRel(planRel.getRel());
            }
        }
        fail("No relations found in plan");
        return null;
    }

    private String findReadRelInRel(Rel rel) {
        if (rel.hasRead()) {
            ReadRel read = rel.getRead();
            if (read.hasNamedTable()) {
                List<String> names = read.getNamedTable().getNamesList();
                assertFalse("Named table should have at least one name", names.isEmpty());
                return names.get(names.size() - 1);
            }
        }
        // Walk into aggregate input
        if (rel.hasAggregate()) {
            return findReadRelInRel(rel.getAggregate().getInput());
        }
        // Walk into project input
        if (rel.hasProject()) {
            return findReadRelInRel(rel.getProject().getInput());
        }
        // Walk into filter input
        if (rel.hasFilter()) {
            return findReadRelInRel(rel.getFilter().getInput());
        }
        fail("No ReadRel found in plan");
        return null;
    }
}
