/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.join;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link CoordinatorHashJoin} — confirms both sides are gathered
 * SINGLETON and the join itself runs at COORDINATOR_REDUCE.
 */
public class CoordinatorHashJoinTests extends OpenSearchTestCase {

    public void testBothSidesSingleton() {
        JoinStrategy strategy = new CoordinatorHashJoin();
        JoinContext ctx = new JoinContext(List.of(0), List.of(0), 1, JoinRelType.INNER);

        ExchangeInfo left = strategy.leftExchange(ctx);
        ExchangeInfo right = strategy.rightExchange(ctx);

        assertEquals(RelDistribution.Type.SINGLETON, left.distributionType());
        assertTrue(left.partitionKeyIndices().isEmpty());
        assertEquals(RelDistribution.Type.SINGLETON, right.distributionType());
        assertTrue(right.partitionKeyIndices().isEmpty());
    }

    public void testExecutesAtCoordinator() {
        JoinStrategy strategy = new CoordinatorHashJoin();
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, strategy.executionType());
    }
}
