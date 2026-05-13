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
 * Distribution policy for a join — decides where the join itself executes. Selected by
 * {@code OpenSearchJoinRule} and attached to {@code OpenSearchJoin}. Adding shuffle /
 * broadcast variants is additive: implement this interface, add selection logic in the rule.
 *
 * @opensearch.internal
 */
public interface JoinStrategy {

    /** Where the join itself executes — coordinator, partition-local, or shard-local. */
    StageExecutionType executionType();
}
