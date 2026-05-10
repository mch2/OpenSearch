/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.join;

import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link CoordinatorHashJoin} — confirms the strategy reports
 * COORDINATOR_REDUCE execution type. Per-side gather placement is no longer the
 * strategy's concern (handled by {@code OpenSearchJoinGatherRule} during Volcano CBO).
 */
public class CoordinatorHashJoinTests extends OpenSearchTestCase {

    public void testExecutesAtCoordinator() {
        JoinStrategy strategy = new CoordinatorHashJoin();
        assertEquals(StageExecutionType.COORDINATOR_REDUCE, strategy.executionType());
    }
}
