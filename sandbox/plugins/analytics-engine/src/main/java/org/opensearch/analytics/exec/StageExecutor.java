/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;

/**
 * Thin router that classifies each stage and delegates to the matching
 * {@link StageScheduler}. Mirrors Trino's
 * {@code PipelinedQueryScheduler.createStageScheduler} shape: a small
 * if-chain that branches on the stage's existing typed fields
 * ({@code ExchangeInfo.distributionType()}, {@link StageExecutionType})
 * and delegates to a purpose-built scheduler.
 *
 * <p>{@link #selectScheduler(Stage)} is the classification method. It reads
 * the stage's typed enums directly — not boolean helpers like
 * {@code isShuffleWrite()} — keeping the router one layer of indirection
 * closer to the planner-set fields. Future specs insert new branches in
 * front of the current ones in priority order (most-specific classifier
 * first):
 * <pre>
 *   // shuffle-exchange-foundation adds:
 *   //   if (exchange != null &amp;&amp; exchange.distributionType() == HASH_DISTRIBUTED)
 *   //       return shuffleWriteScheduler;
 *   //   if (executionType == LOCAL &amp;&amp; hasHashDistributedChild(stage))
 *   //       return shuffleReadScheduler;
 *   //
 *   // broadcast-exchange-foundation adds:
 *   //   if (exchange != null &amp;&amp; exchange.distributionType() == BROADCAST)
 *   //       return broadcastWriteScheduler;
 * </pre>
 *
 * <p>Default scheduler set:
 * <ul>
 *   <li>{@link LocalStageScheduler} — LOCAL pass-through + LOCAL compute</li>
 *   <li>{@link ShardFanOutStageScheduler} — DATA_NODE fan-out to shards</li>
 * </ul>
 *
 * <p>Injected via Guice. Constructor signature unchanged from pre-refactor.
 * Schedulers are constructed inside the constructor body from the injected
 * dependencies — not Guice-managed themselves.
 *
 * @opensearch.internal
 */
public class StageExecutor {

    private static final Logger logger = LogManager.getLogger(StageExecutor.class);

    private final LocalStageScheduler localScheduler;
    private final ShardFanOutStageScheduler shardFanOutScheduler;

    @Inject
    public StageExecutor(ClusterService clusterService, AnalyticsSearchBackendPlugin primaryBackend) {
        this.localScheduler = new LocalStageScheduler(primaryBackend);
        this.shardFanOutScheduler = new ShardFanOutStageScheduler(clusterService);
    }

    /** Test-only — used by ~40 tests that don't exercise LOCAL compute stages. */
    StageExecutor(ClusterService clusterService) {
        this(clusterService, null);
    }

    /**
     * Dispatches a single stage by selecting the appropriate scheduler and
     * delegating. The method signature is unchanged from pre-refactor.
     *
     * @param stage           the stage to dispatch
     * @param outputSink      the sink that this stage's output should feed
     * @param client          outbound shard client for transport dispatch
     * @param childDispatcher callback for recursing on child stages
     * @param config          immutable per-query config
     * @param state           mutable per-query state
     * @param listener        completion listener for this stage
     */
    void dispatch(
        Stage stage,
        ExchangeSink outputSink,
        ShardRequestClient client,
        ChildDispatcher childDispatcher,
        QueryContext config,
        QueryState state,
        ActionListener<Void> listener
    ) {
        selectScheduler(stage).schedule(stage, outputSink, client, childDispatcher, config, state, listener);
    }

    /**
     * Picks the scheduler for a stage. Branches on the stage's existing typed
     * fields: {@link StageExecutionType}.
     *
     * <p>Mirrors Trino's {@code PipelinedQueryScheduler.createStageScheduler}
     * shape: an if-chain that matches on the fragment's output partitioning
     * plus structural stage properties, delegating to a purpose-built scheduler.
     *
     * <p>Branching order follows "most-specific classifier first". Future specs
     * insert their branches in front of the current set:
     * <pre>
     *   // shuffle-exchange-foundation adds:
     *   if (exchange != null &amp;&amp; exchange.distributionType() == HASH_DISTRIBUTED)
     *       return shuffleWriteScheduler;
     *   if (executionType == LOCAL &amp;&amp; hasHashDistributedChild(stage))
     *       return shuffleReadScheduler;
     *
     *   // broadcast-exchange-foundation adds:
     *   if (exchange != null &amp;&amp; exchange.distributionType() == BROADCAST)
     *       return broadcastWriteScheduler;
     * </pre>
     *
     * <p>Package-private so test subclasses can override for spy injection.
     */
    StageScheduler selectScheduler(Stage stage) {
        if (stage.getExecutionType() == StageExecutionType.LOCAL) {
            return localScheduler;
        }
        return shardFanOutScheduler;
    }
}
