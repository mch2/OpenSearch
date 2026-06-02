/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.HashMap;
import java.util.Map;

/**
 * Helpers for shifting {@link RexInputRef} indices when columns are dropped from a row type.
 * Used by {@link ObjectFieldStitch} after stripping {@code ObjectType} columns from a
 * {@link org.apache.calcite.rel.core.TableScan}.
 *
 * @opensearch.internal
 */
final class IndexRemap {

    private IndexRemap() {}

    /**
     * Build {@code oldIdx → newIdx} keyed by field name. Old fields whose names are absent
     * in {@code newType} map to {@code -1} (i.e. dropped).
     */
    static int[] byName(RelDataType oldType, RelDataType newType) {
        Map<String, Integer> newByName = new HashMap<>(newType.getFieldCount());
        for (RelDataTypeField f : newType.getFieldList()) {
            newByName.put(f.getName(), f.getIndex());
        }
        int[] map = new int[oldType.getFieldCount()];
        for (int i = 0; i < map.length; i++) {
            Integer newIdx = newByName.get(oldType.getFieldList().get(i).getName());
            map[i] = newIdx == null ? -1 : newIdx;
        }
        return map;
    }

    /**
     * RexShuttle that rewrites every {@link RexInputRef} via {@code oldToNew}. Throws when
     * an input ref lands on a dropped column — its operator references a column the
     * rewriter removed, which has no defined runtime semantic.
     */
    static RexShuttle shuttle(int[] oldToNew, RelDataType newInputRowType, RexBuilder rexBuilder) {
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int newIdx = oldToNew[ref.getIndex()];
                if (newIdx < 0) {
                    throw new IllegalStateException(
                        "RexInputRef points at a dropped object-parent column; ObjectType cannot appear in filters / aggregates / evals"
                    );
                }
                RelDataType expected = newInputRowType.getFieldList().get(newIdx).getType();
                if (newIdx == ref.getIndex() && expected.equals(ref.getType())) return ref;
                return rexBuilder.makeInputRef(expected, newIdx);
            }
        };
    }
}
