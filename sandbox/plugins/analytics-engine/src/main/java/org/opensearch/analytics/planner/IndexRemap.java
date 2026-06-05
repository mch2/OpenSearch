/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers for shifting {@link RexInputRef} indices when columns are dropped from a row type.
 * Used by {@link ObjectFieldStitch} after stripping {@code ObjectType} columns from a
 * {@link org.apache.calcite.rel.core.TableScan}.
 *
 * <p>Most operators that reference a stripped object-parent column have no defined semantic
 * over an opaque map and are rejected with a clear error. Two contexts <i>do</i> have a
 * sensible expansion and are handled here:
 * <ul>
 *   <li><b>Window {@code PARTITION BY}</b> (PPL {@code | dedup parent}): tuple-equality
 *       semantics, expand to the parent's ordered leaf list.</li>
 *   <li><b>{@code IS NOT NULL} / {@code IS NULL}</b> (PPL {@code | where isnotnull(parent)}):
 *       expand to {@code OR}/{@code AND} of the same predicate over each leaf.</li>
 * </ul>
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
     * RexShuttle that rewrites {@link RexInputRef}s via {@code oldToNew} and expands
     * references to dropped object-parent columns where the surrounding context has a
     * defined leaf-level semantic. Throws {@link IllegalStateException} for any other
     * reference to a dropped column.
     *
     * @param oldToNew old index → new index, {@code -1} for dropped columns
     * @param droppedToLeafIndices for each dropped old index, the ordered list of leaf
     *                             indices in the new row type that flatten that parent
     */
    static RexShuttle shuttle(int[] oldToNew, Map<Integer, List<Integer>> droppedToLeafIndices, RelDataType newRowType, RexBuilder rexBuilder) {
        return new RemapShuttle(oldToNew, droppedToLeafIndices, newRowType, rexBuilder);
    }

    /**
     * Shuttle implementation. Refers to the parent {@link IndexRemap} fields via the
     * constructor; everything is immutable for the lifetime of one rewrite.
     */
    private static final class RemapShuttle extends RexShuttle {

        private final int[] oldToNew;
        private final Map<Integer, List<Integer>> droppedToLeafIndices;
        private final RelDataType newRowType;
        private final RexBuilder rex;

        RemapShuttle(int[] oldToNew, Map<Integer, List<Integer>> droppedToLeafIndices, RelDataType newRowType, RexBuilder rex) {
            this.oldToNew = oldToNew;
            this.droppedToLeafIndices = droppedToLeafIndices;
            this.newRowType = newRowType;
            this.rex = rex;
        }

        @Override
        public RexNode visitInputRef(RexInputRef ref) {
            int newIdx = oldToNew[ref.getIndex()];
            if (newIdx < 0) {
                throw new IllegalStateException(
                    "RexInputRef points at a dropped object-parent column; ObjectType cannot appear in filters / aggregates / evals"
                );
            }
            RelDataType expected = newRowType.getFieldList().get(newIdx).getType();
            if (newIdx == ref.getIndex() && expected.equals(ref.getType())) return ref;
            return rex.makeInputRef(expected, newIdx);
        }

        /**
         * Expand {@code IS [NOT] NULL(parent)} into a disjunction/conjunction over the
         * leaves. Semantic: a parent object "exists" iff any leaf is non-null (matches
         * OpenSearch's {@code _exists_:parent}); "is null" iff every leaf is null.
         */
        @Override
        public RexNode visitCall(RexCall call) {
            if (isNullCheckOnDroppedParent(call)) {
                int oldIdx = ((RexInputRef) call.getOperands().get(0)).getIndex();
                List<RexNode> perLeaf = makePerLeafCalls(oldIdx, call.getOperator());
                boolean isNotNull = call.getOperator() == SqlStdOperatorTable.IS_NOT_NULL;
                return rex.makeCall(isNotNull ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND, perLeaf);
            }
            return super.visitCall(call);
        }

        /**
         * Expand each partition-by key that points at a dropped parent into its leaf list,
         * then sweep the rest of the {@link RexOver} structure (operands, ORDER BY) through
         * this shuttle so any embedded refs to surviving columns are remapped correctly.
         */
        @Override
        public RexNode visitOver(RexOver over) {
            RexWindow window = over.getWindow();
            ExpandedKeys expandedKeys = expandPartitionKeys(window.partitionKeys);
            if (!expandedKeys.expanded) return super.visitOver(over);

            List<RexNode> newOperands = visitList(over.getOperands(), new boolean[1]);
            List<RexFieldCollation> newOrderKeys = new ArrayList<>(window.orderKeys.size());
            for (RexFieldCollation oc : window.orderKeys) {
                newOrderKeys.add(new RexFieldCollation(oc.left.accept(this), oc.right));
            }
            return rex.makeOver(
                over.getType(),
                over.getAggOperator(),
                newOperands,
                expandedKeys.keys,
                ImmutableList.copyOf(newOrderKeys),
                window.getLowerBound(),
                window.getUpperBound(),
                window.isRows(),
                /*allowPartial*/ true,
                /*nullWhenCountZero*/ false,
                over.isDistinct(),
                over.ignoreNulls()
            );
        }

        /** {@code true} if {@code call} is a 1-arg {@code IS [NOT] NULL} on a dropped column. */
        private boolean isNullCheckOnDroppedParent(RexCall call) {
            return (call.getOperator() == SqlStdOperatorTable.IS_NOT_NULL || call.getOperator() == SqlStdOperatorTable.IS_NULL)
                && call.getOperands().size() == 1
                && call.getOperands().get(0) instanceof RexInputRef ref
                && oldToNew[ref.getIndex()] < 0;
        }

        /** Build {@code op(leafRef)} for each leaf flattened from the dropped parent at {@code oldIdx}. */
        private List<RexNode> makePerLeafCalls(int oldIdx, org.apache.calcite.sql.SqlOperator op) {
            List<Integer> leaves = leavesOrThrow(oldIdx, "IS [NOT] NULL on object parent");
            List<RexNode> perLeaf = new ArrayList<>(leaves.size());
            for (int leafIdx : leaves) {
                perLeaf.add(rex.makeCall(op, leafRefAt(leafIdx)));
            }
            return perLeaf;
        }

        /** Walk partition keys; each key on a dropped parent becomes its leaf list. */
        private ExpandedKeys expandPartitionKeys(List<RexNode> origKeys) {
            List<RexNode> out = new ArrayList<>(origKeys.size());
            boolean expanded = false;
            for (RexNode key : origKeys) {
                if (key instanceof RexInputRef ref && oldToNew[ref.getIndex()] < 0) {
                    for (int leafIdx : leavesOrThrow(ref.getIndex(), "RexOver PARTITION BY on object parent")) {
                        out.add(leafRefAt(leafIdx));
                    }
                    expanded = true;
                } else {
                    out.add(key.accept(this));
                }
            }
            return new ExpandedKeys(out, expanded);
        }

        private List<Integer> leavesOrThrow(int oldIdx, String context) {
            List<Integer> leaves = droppedToLeafIndices.get(oldIdx);
            if (leaves == null || leaves.isEmpty()) {
                throw new IllegalStateException(context + " without expandable leaves");
            }
            return leaves;
        }

        private RexNode leafRefAt(int newIdx) {
            return rex.makeInputRef(newRowType.getFieldList().get(newIdx).getType(), newIdx);
        }

        /** Result of {@link #expandPartitionKeys}: the new key list and whether anything changed. */
        private record ExpandedKeys(List<RexNode> keys, boolean expanded) {}
    }
}
