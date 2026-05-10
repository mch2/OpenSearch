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
 * Distribution policy for a join. Decides where the join itself runs. Per-side gather
 * placement is no longer the strategy's concern — {@code OpenSearchJoinGatherRule} drives
 * exchange insertion via Volcano CBO {@code convert(input, SINGLETON)}.
 *
 * <p>The rule (see {@code OpenSearchJoinRule}) selects a strategy and attaches it to
 * {@code OpenSearchJoin}. Adding a new strategy (shuffle / broadcast) is purely additive:
 * implement this interface, register selection logic in the rule.
 *
 * @opensearch.internal
 */
public interface JoinStrategy {

    /** Where the join itself executes — coordinator, partition-local, or shard-local. */
    StageExecutionType executionType();
}
