/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;
import java.util.Objects;

/**
 * Distribution trait for OpenSearch Analytics operators.
 *
 * <p>Carries Calcite's {@link Type} (SINGLETON / RANDOM / HASH / ANY), optional
 * HASH keys, and — for SINGLETON only — an {@link Origin} distinguishing "data
 * already at one node because of storage layout" (SCAN, single-shard scan) from
 * "data gathered onto one node by a runtime exchange" (GATHERED, ER / FINAL
 * aggregate output).
 *
 * <p>The origin lets ConverterImpl-based ExchangeReducer dedupe correctly: an ER
 * over a GATHERED input is redundant (two ERs in a row), but an ER over a SCAN
 * input is a real stage boundary and must survive.
 *
 * @opensearch.internal
 */
@SuppressWarnings("unchecked")
public class OpenSearchDistribution implements RelDistribution {

    /**
     * For SINGLETON only. SCAN = data happens to live on one node (single-shard scan).
     * GATHERED = runtime exchange produced this SINGLETON. Null for non-SINGLETON types.
     */
    public enum Origin {
        SCAN,
        GATHERED
    }

    private final OpenSearchDistributionTraitDef traitDef;
    private final Type type;
    private final List<Integer> keys;
    private final Origin origin;

    OpenSearchDistribution(OpenSearchDistributionTraitDef traitDef, Type type, List<Integer> keys, Origin origin) {
        this.traitDef = traitDef;
        this.type = type;
        this.keys = keys;
        this.origin = origin;
    }

    public Origin getOrigin() {
        return origin;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public List<Integer> getKeys() {
        return keys;
    }

    @Override
    public RelTraitDef<? extends RelTrait> getTraitDef() {
        return traitDef;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        if (!(trait instanceof OpenSearchDistribution other)) {
            return false;
        }
        if (other.type == Type.ANY) {
            return true;
        }
        if (this.type != other.type || !this.keys.equals(other.keys)) {
            return false;
        }
        // SINGLETON origin: demand with null origin accepts any origin (used by root demand
        // and DeriveRule — "get me onto one node, don't care how"). Demand with a specific
        // origin requires exact match — so SINGLETON(GATHERED) demand by a HEP-wrapped ER
        // does NOT dedupe into a SINGLETON(SCAN) subset, keeping the ER over single-shard
        // scans as a real stage boundary.
        if (this.type == Type.SINGLETON && other.origin != null) {
            return this.origin == other.origin;
        }
        return true;
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public RelDistribution apply(Mappings.TargetMapping mapping) {
        if (type != Type.HASH_DISTRIBUTED || keys.isEmpty()) {
            return this;
        }
        // Calcite's contract on RelDistribution.apply (RelDistribution.java:53-67) is to
        // silently degrade to ANY if any HASH key cannot be mapped through the projection.
        // Mappings.apply2 throws on an unmapped key, which is the wrong behavior here — fall
        // back to ANY when the mapping drops a key we depend on.
        List<Integer> newKeys = new java.util.ArrayList<>(keys.size());
        for (int key : keys) {
            int target = mapping.getTargetOpt(key);
            if (target < 0) {
                return new OpenSearchDistribution(traitDef, Type.ANY, List.of(), null);
            }
            newKeys.add(target);
        }
        return new OpenSearchDistribution(traitDef, Type.HASH_DISTRIBUTED, newKeys, null);
    }

    @Override
    public boolean isTop() {
        return type == Type.ANY;
    }

    @Override
    public int compareTo(org.apache.calcite.plan.RelMultipleTrait other) {
        if (other instanceof OpenSearchDistribution otherDist) {
            return type.compareTo(otherDist.type);
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof OpenSearchDistribution other)) return false;
        return type == other.type && Objects.equals(keys, other.keys) && origin == other.origin;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, keys, origin);
    }

    @Override
    public String toString() {
        return switch (type) {
            case SINGLETON -> origin == null ? "SINGLETON" : "SINGLETON(" + origin + ")";
            case RANDOM_DISTRIBUTED -> "RANDOM";
            case HASH_DISTRIBUTED -> "HASH" + keys;
            case ANY -> "ANY";
            default -> type.shortName;
        };
    }
}
