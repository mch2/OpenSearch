/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionListener;

/**
 * Strategy for dispatching one stage. Each implementation owns the decision
 * of "given a stage of shape X, how do I turn it into executing tasks?".
 *
 * <p>Implementations construct the appropriate {@link StageExecution},
 * register it in {@link QueryState#stageExecutions}, and invoke it.
 *
 * <p>Classification — "which scheduler handles this stage?" — is the
 * responsibility of {@code StageExecutor.selectScheduler}, not the scheduler
 * itself. Schedulers do not carry predicates or priorities; they just do
 * the dispatch work they were constructed for. This mirrors Trino's
 * {@code PipelinedQueryScheduler.createStageScheduler}, which uses an
 * if-chain on the fragment's {@code PartitioningHandle} to pick a
 * {@code StageScheduler} impl.
 *
 * <p>Default implementations:
 * <ul>
 *   <li>{@code LocalStageScheduler} — LOCAL pass-through + LOCAL compute</li>
 *   <li>{@code ShardFanOutStageScheduler} — DATA_NODE fan-out to shards</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface StageScheduler {

    void schedule(
        Stage stage,
        ExchangeSink outputSink,
        ShardRequestClient client,
        ChildDispatcher childDispatcher,
        QueryContext config,
        QueryState state,
        ActionListener<Void> listener
    );
}
