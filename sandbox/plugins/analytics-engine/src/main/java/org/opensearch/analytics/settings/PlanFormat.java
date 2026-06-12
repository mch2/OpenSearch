/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import java.util.Locale;

/**
 * Stage-boundary plan format, selected per stage kind by the
 * {@code analytics.engine.plan_format} setting (df-proto migration spec D12).
 *
 * <ul>
 *   <li>{@link #LEGACY} — every stage ships a Substrait logical fragment plus side
 *       channels (instructions, delegation descriptor, schema stubs). The default.</li>
 *   <li>{@link #REDUCE_PROTO} — reduce / coordinator-local stages ship a serialized
 *       DataFusion physical plan ({@code datafusion-proto}); shard stages remain
 *       byte-identical legacy. Mixed formats are safe — the inter-stage boundary is
 *       Arrow partition streams either way.</li>
 *   <li>{@link #FULL_PROTO} — all stages, including shard stages, ship proto plans.</li>
 * </ul>
 */
public enum PlanFormat {
    LEGACY,
    REDUCE_PROTO,
    FULL_PROTO;

    /** Parse the lower-case setting token ({@code legacy|reduce_proto|full_proto}). */
    public static PlanFormat fromString(String value) {
        if (value == null) {
            return LEGACY;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "legacy" -> LEGACY;
            case "reduce_proto" -> REDUCE_PROTO;
            case "full_proto" -> FULL_PROTO;
            default -> throw new IllegalArgumentException(
                "Invalid analytics.engine.plan_format ["
                    + value
                    + "]; expected one of legacy, reduce_proto, full_proto"
            );
        };
    }

    /** The setting token for this format (inverse of {@link #fromString}). */
    public String token() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** True if reduce / coordinator-local stages should be finalized to proto. */
    public boolean reduceStagesProto() {
        return this == REDUCE_PROTO || this == FULL_PROTO;
    }

    /** True if shard stages should be finalized to proto. */
    public boolean shardStagesProto() {
        return this == FULL_PROTO;
    }
}
