/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Per-query shared state passed from {@link StageExecutor} to each {@link StageExecution}.
 * Immutable references — the collections themselves are mutable and shared across stages.
 *
 * @opensearch.internal
 */
record QueryExecutionContext(
    String queryId,
    Executor searchExecutor,
    ExchangeSink rootSink,
    Set<Integer> completedStages,
    Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests,
    Task parentTask
) {
}
