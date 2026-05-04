/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link org.opensearch.analytics.planner.rules.OpenSearchUnionRule}: matches
 * standard Calcite {@link LogicalUnion}, produces an {@link OpenSearchUnion} with every
 * input SINGLETON-converted (wrapped in {@link OpenSearchExchangeReducer} by Volcano).
 *
 * <p>PPL commands that lower to UNION ALL — {@code addcoltotals}, {@code addtotals},
 * {@code append}, {@code appendpipe}, {@code multisearch} — previously left the
 * LogicalUnion unmarked, propagating to downstream rules as {@code <rule> encountered
 * unmarked child [LogicalUnion]}. The new rule descends through unions the same way
 * {@link org.opensearch.analytics.planner.rules.OpenSearchJoinRule} descends through
 * joins.
 */
public class UnionRuleTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(UnionRuleTests.class);

    public void testUnionAllMatchesAndProducesOpenSearchUnion() {
        RelNode result = runUnion(true);
        assertUnionMarked(result, true);
    }

    public void testDistinctUnionMatchesAndProducesOpenSearchUnion() {
        RelNode result = runUnion(false);
        assertUnionMarked(result, false);
    }

    private void assertUnionMarked(RelNode root, boolean expectedAll) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(root);
        if (unwrapped instanceof OpenSearchExchangeReducer wrapper) {
            unwrapped = RelNodeUtils.unwrapHep(wrapper.getInput());
        }
        assertTrue(
            "rule should produce OpenSearchUnion, got " + unwrapped.getClass().getSimpleName(),
            unwrapped instanceof OpenSearchUnion
        );

        OpenSearchUnion osUnion = (OpenSearchUnion) unwrapped;
        assertEquals("union `all` flag preserved", expectedAll, osUnion.all);
        for (int i = 0; i < osUnion.getInputs().size(); i++) {
            RelNode input = RelNodeUtils.unwrapHep(osUnion.getInputs().get(i));
            assertTrue(
                "input " + i + " wrapped in OpenSearchExchangeReducer, got " + input.getClass().getSimpleName(),
                input instanceof OpenSearchExchangeReducer
            );
        }
    }

    private RelNode runUnion(boolean all) {
        PlannerContext context = buildContext(
            "parquet",
            2,
            Map.of("k", Map.of("type", "integer"))
        );
        RelOptTable table = mockTable("test_index", "k");
        RelNode left = stubScan(table);
        RelNode right = stubScan(table);
        LogicalUnion union = LogicalUnion.create(List.of(left, right), all);

        LOGGER.info("Input union:\n{}", RelOptUtil.toString(union));
        RelNode result = runPlanner(union, context);
        LOGGER.info("Marked+CBO output:\n{}", RelOptUtil.toString(result));
        return result;
    }
}
