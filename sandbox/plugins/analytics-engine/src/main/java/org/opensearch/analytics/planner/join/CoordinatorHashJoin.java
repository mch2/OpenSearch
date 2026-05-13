/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.join;

import org.opensearch.analytics.planner.dag.StageExecutionType;

/**
 * Coordinator-side hash join: both inputs gathered SINGLETON to coord, which builds a
 * hash table on the right input and probes with the left. The only strategy today;
 * shuffle and broadcast variants slot in via {@link JoinStrategy}.
 *
 * @opensearch.internal
 */
public final class CoordinatorHashJoin implements JoinStrategy {

    @Override
    public StageExecutionType executionType() {
        return StageExecutionType.COORDINATOR_REDUCE;
    }
}
