/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.join;

import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.StageExecutionType;

/**
 * Coordinator-side hash join: both inputs are gathered SINGLETON to the coordinator,
 * which builds a hash table on the right input and probes with the left. This is
 * the only strategy implemented today; shuffle and broadcast variants slot in via
 * {@link JoinStrategy} when they're added.
 *
 * @opensearch.internal
 */
public final class CoordinatorHashJoin implements JoinStrategy {

    @Override
    public ExchangeInfo leftExchange(JoinContext ctx) {
        return ExchangeInfo.singleton();
    }

    @Override
    public ExchangeInfo rightExchange(JoinContext ctx) {
        return ExchangeInfo.singleton();
    }

    @Override
    public StageExecutionType executionType() {
        return StageExecutionType.COORDINATOR_REDUCE;
    }
}
