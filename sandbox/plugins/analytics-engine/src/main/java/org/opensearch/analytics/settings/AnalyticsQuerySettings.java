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
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

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
     * Multiplier applied to a result's estimated <em>native</em> Arrow footprint to approximate its
     * on-heap Java-object size when reserving against the shared {@code request} circuit breaker.
     * Java rows (boxed values, String objects, {@code Object[]} arrays) cost more than the packed
     * native buffers, and the reservation must also cover the downstream response copy; a factor
     * &gt; 1 leaves that headroom. Applied symmetrically at upfront admission and at
     * {@code shrinkTo} of the actual size. Default 1.5. Dynamic + NodeScope.
     */
    public static final Setting<Double> RESULT_HEAP_EXPANSION_FACTOR = Setting.doubleSetting(
        "analytics.query.result_heap_expansion_factor",
        1.5,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Assumed per-value byte width for variable-width result columns (VARCHAR, VARBINARY, etc.) in the
     * upfront worst-case admission estimate, where the true value sizes are not yet known. Fixed-width
     * columns use their exact Arrow buffer width; only variable-width columns rely on this allowance.
     * Size it for the workload's typical string width — log/message fields are commonly &gt; 256B, so
     * a too-small value under-estimates and the admission guard admits queries it should shed. Default
     * 1 KB. Dynamic + NodeScope.
     */
    public static final Setting<ByteSizeValue> RESULT_VARWIDTH_ALLOWANCE = Setting.byteSizeSetting(
        "analytics.query.result_varwidth_allowance",
        new ByteSizeValue(1, ByteSizeUnit.KB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Max rows a single query result may materialize on the coordinator heap. Single source of truth:
     * {@code RowProducingSink} enforces it (rejecting further batches) and the admission guard's
     * worst-case estimate multiplies by it, so a query is admitted iff its capped result could fit.
     * Default 10_000. Dynamic + NodeScope.
     */
    public static final Setting<Integer> MAX_RESULT_ROWS = Setting.intSetting(
        "analytics.query.max_result_rows",
        10_000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> all() {
        return List.of(
            DELEGATION_BLOCKED_PREDICATES,
            MAX_SHARDS_PER_QUERY,
            MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE,
            RESULT_HEAP_EXPANSION_FACTOR,
            RESULT_VARWIDTH_ALLOWANCE,
            MAX_RESULT_ROWS
        );
    }

    private AnalyticsQuerySettings() {}
}
