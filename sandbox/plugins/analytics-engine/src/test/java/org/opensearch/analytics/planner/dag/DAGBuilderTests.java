/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Tests for {@link DAGBuilder} — verifies correct stage structure for single-stage
 * and two-stage query shapes.
 */
public class DAGBuilderTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(DAGBuilderTests.class);

    private QueryDAG buildDAG(int shardCount, RelNode logicalPlan) {
        var context = buildContext("parquet", shardCount, intFields());
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("QueryDAG:\n{}", dag);
        return dag;
    }

    private static void assertBottomUpIds(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            assertTrue("child stageId must be lower than parent", child.getStageId() < stage.getStageId());
            assertBottomUpIds(child);
        }
    }

    /**
     * Single-shard scan and aggregate both produce one stage with a shard target resolver
     * and no exchange sink — no coordinator stage needed.
     */
    public void testSingleStageQueries() {
        QueryDAG scanDag = buildDAG(1, stubScan(mockTable("test_index", "status", "size")));
        assertEquals(0, scanDag.rootStage().getChildStages().size());
        assertNotNull(scanDag.rootStage().getTargetResolver());
        assertNull(scanDag.rootStage().getExchangeSinkProvider());

        QueryDAG aggDag = buildDAG(1, makeAggregate(sumCall()));
        assertEquals(0, aggDag.rootStage().getChildStages().size());
        assertNotNull(aggDag.rootStage().getTargetResolver());
        assertNull(aggDag.rootStage().getExchangeSinkProvider());

        // Sort(Filter(Scan)) with limit — single stage, sort-capable backend
        QueryDAG sortDag = buildDAG(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10)
        );
        assertEquals(0, sortDag.rootStage().getChildStages().size());
        assertNotNull(sortDag.rootStage().getTargetResolver());
        assertNull(sortDag.rootStage().getExchangeSinkProvider());
        assertTrue(sortDag.rootStage().getFragment() instanceof OpenSearchSort);
    }

    /**
     * Multi-shard scan and aggregate both produce two stages. Verifies coordinator root
     * structure (ExchangeReducer → StageInputScan, null targetResolver) and child structure
     * (TableScan leaf, non-null targetResolver, correct ExchangeInfo).
     */
    public void testTwoStageQueries() {
        // Multi-shard scan: pure gather, no compute at coordinator
        QueryDAG scanDag = buildDAG(5, stubScan(mockTable("test_index", "status", "size")));
        assertBottomUpIds(scanDag.rootStage());
        assertEquals(1, scanDag.rootStage().getChildStages().size());
        assertNull(scanDag.rootStage().getTargetResolver());
        assertTrue(scanDag.rootStage().getFragment() instanceof OpenSearchExchangeReducer);
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) scanDag.rootStage().getFragment();
        assertTrue(reducer.getInput() instanceof OpenSearchStageInputScan);
        Stage scanChild = scanDag.rootStage().getChildStages().get(0);
        assertNotNull(scanChild.getTargetResolver());
        assertTrue(scanChild.getFragment() instanceof OpenSearchTableScan);

        // Multi-shard aggregate: coordinator reduces partial aggregates
        QueryDAG aggDag = buildDAG(2, makeAggregate(sumCall()));
        assertBottomUpIds(aggDag.rootStage());
        assertEquals(1, aggDag.rootStage().getChildStages().size());
        assertNull(aggDag.rootStage().getTargetResolver());
        assertNotNull(aggDag.rootStage().getExchangeSinkProvider());
        Stage aggChild = aggDag.rootStage().getChildStages().get(0);
        assertNotNull(aggChild.getTargetResolver());
        assertNull(aggChild.getExchangeSinkProvider());
        assertNotNull(aggChild.getExchangeInfo());
        assertEquals(RelDistribution.Type.SINGLETON, aggChild.getExchangeInfo().distributionType());
    }

    /**
     * Verifies DAGBuilder handles a parent with two ExchangeReducer inputs (the join shape).
     * Synthesizes the post-CBO marked plan directly — no planner rule yet.
     *
     * <p>Expected DAG: child stage 0 (t1 scan), child stage 1 (t2 scan), root stage 2 (join).
     * Root fragment must be an OpenSearchJoin whose left and right inputs are both
     * OpenSearchExchangeReducer wrapping an OpenSearchStageInputScan referencing the
     * correct child stage id.
     */
    public void testJoinUnderRootProducesThreeStages() {
        PlannerContext context = buildContext("parquet", 2, intFields());
        List<String> viable = List.of(MockDataFusionBackend.NAME);
        List<FieldStorageInfo> emptyStorage = List.of();

        OpenSearchTableScan leftScan = new OpenSearchTableScan(
            cluster,
            cluster.traitSet(),
            mockTable("t1", "k", "v"),
            viable,
            emptyStorage
        );
        OpenSearchTableScan rightScan = new OpenSearchTableScan(
            cluster,
            cluster.traitSet(),
            mockTable("t2", "k", "w"),
            viable,
            emptyStorage
        );
        OpenSearchExchangeReducer leftReducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), leftScan, viable);
        OpenSearchExchangeReducer rightReducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), rightScan, viable);

        // Inner equi-join on t1.k = t2.k. Field 0 of left = field 0 of right (offset by left fieldCount=2).
        RexNode joinCondition = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        OpenSearchJoin join = new OpenSearchJoin(
            cluster,
            cluster.traitSet(),
            leftReducer,
            rightReducer,
            joinCondition,
            JoinRelType.INNER,
            viable
        );

        QueryDAG dag = DAGBuilder.build(join, context.getCapabilityRegistry(), mockClusterService());
        LOGGER.info("Join QueryDAG:\n{}", dag);

        Stage root = dag.rootStage();
        assertBottomUpIds(root);
        assertEquals("root has two child stages", 2, root.getChildStages().size());
        assertNull("root has no shard target resolver", root.getTargetResolver());
        assertNotNull("root has an exchange sink provider", root.getExchangeSinkProvider());
        assertTrue("root fragment is OpenSearchJoin", root.getFragment() instanceof OpenSearchJoin);

        OpenSearchJoin rootJoin = (OpenSearchJoin) root.getFragment();
        assertTrue(
            "left input rewritten to ExchangeReducer→StageInputScan",
            rootJoin.getLeft() instanceof OpenSearchExchangeReducer
        );
        assertTrue(
            "right input rewritten to ExchangeReducer→StageInputScan",
            rootJoin.getRight() instanceof OpenSearchExchangeReducer
        );
        OpenSearchExchangeReducer leftRoot = (OpenSearchExchangeReducer) rootJoin.getLeft();
        OpenSearchExchangeReducer rightRoot = (OpenSearchExchangeReducer) rootJoin.getRight();
        assertTrue(leftRoot.getInput() instanceof OpenSearchStageInputScan);
        assertTrue(rightRoot.getInput() instanceof OpenSearchStageInputScan);
        OpenSearchStageInputScan leftStageInput = (OpenSearchStageInputScan) leftRoot.getInput();
        OpenSearchStageInputScan rightStageInput = (OpenSearchStageInputScan) rightRoot.getInput();

        Stage leftChild = root.getChildStages().get(0);
        Stage rightChild = root.getChildStages().get(1);
        assertNotEquals("child stage ids are distinct", leftChild.getStageId(), rightChild.getStageId());
        assertEquals(
            "left StageInputScan references the left child stage",
            leftChild.getStageId(),
            leftStageInput.getChildStageId()
        );
        assertEquals(
            "right StageInputScan references the right child stage",
            rightChild.getStageId(),
            rightStageInput.getChildStageId()
        );

        // Each child stage is a SHARD_FRAGMENT with its own target resolver and the original table scan.
        assertNotNull("left child has shard target resolver", leftChild.getTargetResolver());
        assertNotNull("right child has shard target resolver", rightChild.getTargetResolver());
        assertTrue(leftChild.getFragment() instanceof OpenSearchTableScan);
        assertTrue(rightChild.getFragment() instanceof OpenSearchTableScan);
        assertEquals(RelDistribution.Type.SINGLETON, leftChild.getExchangeInfo().distributionType());
        assertEquals(RelDistribution.Type.SINGLETON, rightChild.getExchangeInfo().distributionType());
    }
}
