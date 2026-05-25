/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Settings that trade accuracy for memory/network in the analytics engine.
 *
 * @opensearch.internal
 */
public final class AnalyticsApproximationSettings {

    private AnalyticsApproximationSettings() {}

    /**
     * Per-shard bucket oversampling factor for distributed aggregations. Each shard ships
     * {@code ceil(max(LIMIT, 10) * factor) + 10} buckets for {@code GROUP BY k ORDER BY agg
     * LIMIT N} shapes, mirroring native OpenSearch's terms-aggregation {@code shard_size}.
     * Higher values reduce approximation error at the cost of shard memory + network.
     *
     * <p>Allowed: {@code 0.0} (disabled, exact gather) or {@code >= 1.0}. Default {@code 1.5}.
     * {@code (0.0, 1.0)} is rejected — sub-1 multipliers would shrink rather than oversample.
     */
    public static final Setting<Double> INDEX_ANALYTICS_SHARD_BUCKET_OVERSAMPLING_FACTOR = new Setting<>(
        "index.analytics.shard_bucket_oversampling_factor",
        Double.toString(1.5),
        s -> {
            double v = Double.parseDouble(s);
            if (v < 0.0 || (v > 0.0 && v < 1.0)) {
                throw new IllegalArgumentException(
                    "[index.analytics.shard_bucket_oversampling_factor] must be 0.0 (disabled) or >= 1.0; got " + v
                );
            }
            return v;
        },
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    /** All settings declared in this category — registered by {@code AnalyticsPlugin#getSettings()}. */
    public static List<Setting<?>> all() {
        return List.of(INDEX_ANALYTICS_SHARD_BUCKET_OVERSAMPLING_FACTOR);
    }
}
