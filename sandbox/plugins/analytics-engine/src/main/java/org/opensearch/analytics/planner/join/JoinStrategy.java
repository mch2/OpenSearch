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
import org.opensearch.analytics.spi.JoinAlgorithm;

/**
 * Distribution policy for a join. Decides how each side of the join is delivered
 * to the join executor and where the join itself runs.
 *
 * <p>The rule (see {@code OpenSearchJoinRule}) selects a strategy and attaches it
 * to {@code OpenSearchJoin}; the strategy then controls the {@link ExchangeInfo}
 * on each input's {@code OpenSearchExchangeReducer}. {@code DAGBuilder} reads the
 * exchange info off the reducer when cutting child stages — it never inspects the
 * join's strategy directly. Adding a new strategy (shuffle / broadcast) is purely
 * additive: implement this interface, register selection logic in the rule.
 *
 * <p>Each side is queried independently because broadcast joins have asymmetric
 * distribution (probe side stays on shards, build side broadcasts).
 *
 * @opensearch.internal
 */
public interface JoinStrategy {

    /** How the left input is distributed to the join executor(s). */
    ExchangeInfo leftExchange(JoinContext ctx);

    /** How the right (build-side) input is distributed to the join executor(s). */
    ExchangeInfo rightExchange(JoinContext ctx);

    /** Where the join itself executes — coordinator, partition-local, or shard-local. */
    StageExecutionType executionType();

    /** The execution algorithm. Used to pick a backend that declares matching support. */
    JoinAlgorithm algorithm();
}
