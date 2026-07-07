/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.settings.Setting;

import java.util.List;

/** Cluster-level settings for analytics query execution limits. */
public final class AnalyticsQuerySettings {

    /** Affix-setting prefix; full key is {@code analytics.delegation.<backend>.blocked_predicates}. */
    public static final String DELEGATION_BLOCKED_PREDICATES_PREFIX = "analytics.delegation.";

    /**
     * Per-backend block-list of predicate functions that must NOT be delegated to that backend. Affix
     * (namespaced) setting: the backend name is the namespace, the value is a list of
     * {@link ScalarFunction} names (case-insensitive). Models the operator-facing
     * {@code Map<BackendName, List<BlockedPredicate>>} contract.
     *
     * <pre>
     * analytics.delegation.lucene.blocked_predicates:  ["LIKE","EQUALS"]
     * </pre>
     *
     * <p>Default empty. Enforced at the marking layer ({@code OpenSearchFilterRule}): a blocked
     * predicate is dropped from that backend's viable set, so the planner leaves it on a non-blocked
     * backend. Dynamic + NodeScope. Registry-derived validation (namespace must be a FILTER-delegation
     * acceptor; predicate must have a serializer on that backend) runs in {@code DelegationBlockList}.
     */
    public static final Setting.AffixSetting<List<ScalarFunction>> DELEGATION_BLOCKED_PREDICATES = Setting.affixKeySetting(
        DELEGATION_BLOCKED_PREDICATES_PREFIX,
        "blocked_predicates",
        key -> Setting.listSetting(
            key,
            key.contains("lucene")
                ? List.of(
                    "IS_NULL",
                    "IS_NOT_NULL",
                    "NOT_EQUALS",
                    "LIKE",
                    "GREATER_THAN",
                    "GREATER_THAN_OR_EQUAL",
                    "LESS_THAN",
                    "LESS_THAN_OR_EQUAL",
                    "SARG_PREDICATE"
                )
                : List.of(),
            ScalarFunction::fromToken,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    );

    public static final Setting<Integer> MAX_SHARDS_PER_QUERY = Setting.intSetting(
        "analytics.query.max_shards_per_query",
        50,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Max in-flight shard fragment requests <b>per data node</b> for a single query. The coordinator
     * keeps an independent throttle per target node, so total in-flight requests for a query can be
     * up to this value times the number of nodes it fans out to — this bounds the load any single
     * node sees, not the query's overall concurrency.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE = Setting.intSetting(
        "analytics.query.max_concurrent_shard_requests_per_node",
        5,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * When true, the analytics-engine bypasses the Java distributed planner/scheduler
     * ({@code DAGBuilder}/{@code PlanForker}/{@code FragmentConversionDriver}/{@code QueryScheduler})
     * and instead emits one whole-query Substrait plan that is executed through the Rust
     * {@code datafusion-distributed} engine (direct rust↔rust gRPC data plane). Off by default;
     * dynamic so it can be flipped per cluster while the new path bakes. See
     * {@code DefaultPlanExecutor.executeInternalDistributed}.
     */
    public static final Setting<Boolean> DISTRIBUTED_ENGINE = Setting.boolSetting(
        "analytics.query.distributed_engine",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Distributed-planner tuning (datafusion-distributed's {@code DistributedConfig}), read per query
     * in {@code DefaultPlanExecutor} and passed via {@code DistributedTuning}. Only affects the
     * distributed path ({@link #DISTRIBUTED_ENGINE}); no effect on the legacy path.
     */

    /**
     * Insert an intermediate {@code PartialReduce} above each hash repartition, before the network
     * shuffle, so high-cardinality group-bys merge partials locally and the shuffle carries fewer rows.
     * Default true (helps the wide-key case; slight overhead when aggregation doesn't reduce much).
     */
    public static final Setting<Boolean> DISTRIBUTED_PARTIAL_REDUCE = Setting.boolSetting(
        "analytics.query.distributed.partial_reduce",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Force PARTITIONED hash joins (both sides hash-repartitioned on the join key) rather than
     * CollectLeft broadcast. Default true — REQUIRED for correctness on the distributed path (a
     * CollectLeft join is capped to one task while our leaves are per-shard, so it would see only some
     * shards' rows). Exposed as an escape hatch; changing it risks wrong join results.
     */
    public static final Setting<Boolean> DISTRIBUTED_FORCE_PARTITIONED_JOINS = Setting.boolSetting(
        "analytics.query.distributed.force_partitioned_joins",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Multiplier applied to a stage's task count when a node increases cardinality (&gt;1 fans wider so
     * a wide reduce/join spreads across more workers). {@code 0} keeps the library default.
     */
    public static final Setting<Double> DISTRIBUTED_CARDINALITY_TASK_COUNT_FACTOR = Setting.doubleSetting(
        "analytics.query.distributed.cardinality_task_count_factor",
        0.0,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Hard cap on tasks per distributed stage. {@code 0} inherits the worker count. */
    public static final Setting<Integer> DISTRIBUTED_MAX_TASKS_PER_STAGE = Setting.intSetting(
        "analytics.query.distributed.max_tasks_per_stage",
        0,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> all() {
        return List.of(
            DELEGATION_BLOCKED_PREDICATES,
            MAX_SHARDS_PER_QUERY,
            MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE,
            DISTRIBUTED_ENGINE,
            DISTRIBUTED_PARTIAL_REDUCE,
            DISTRIBUTED_FORCE_PARTITIONED_JOINS,
            DISTRIBUTED_CARDINALITY_TASK_COUNT_FACTOR,
            DISTRIBUTED_MAX_TASKS_PER_STAGE
        );
    }

    private AnalyticsQuerySettings() {}
}
