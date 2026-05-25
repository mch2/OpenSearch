/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Pre-isthmus pass that rewrites every {@link OpenSearchSort} with
 * {@link OpenSearchSort#hasExpressionCollation()} into a
 * {@code Project(drop) → Sort(by lifted columns) → Project(lift sortExprs)} chain so the
 * isthmus visitor emits standard substrait constructs. The drop Project keeps the on-wire
 * schema unchanged.
 *
 * @opensearch.internal
 */
final class OpenSearchSortExpressionRewriter {

    private OpenSearchSortExpressionRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                if (visited instanceof OpenSearchSort sort && sort.hasExpressionCollation()) {
                    return rewriteSort(sort);
                }
                return visited;
            }
        });
    }

    private static RelNode rewriteSort(OpenSearchSort sort) {
        RelNode child = sort.getInput();
        RexBuilder rb = sort.getCluster().getRexBuilder();
        List<RexNode> sortExprs = sort.getSortExprs();
        int origColCount = child.getRowType().getFieldCount();

        // Lift Project: add each sortExpr as a new column with a synthesized name.
        List<RexNode> liftExprs = new ArrayList<>(origColCount + sortExprs.size());
        List<String> liftNames = new ArrayList<>(origColCount + sortExprs.size());
        Set<String> usedNames = new HashSet<>();
        for (int i = 0; i < origColCount; i++) {
            RelDataTypeField f = child.getRowType().getFieldList().get(i);
            liftExprs.add(rb.makeInputRef(f.getType(), i));
            liftNames.add(f.getName());
            usedNames.add(f.getName());
        }
        for (int i = 0; i < sortExprs.size(); i++) {
            liftExprs.add(sortExprs.get(i));
            liftNames.add(uniqueName("$shard_sort_key_" + i, usedNames));
        }
        RelNode lift = LogicalProject.create(child, List.of(), liftExprs, liftNames, Set.of());

        // Sort: collation references lifted columns at indices [origColCount, origColCount+N).
        List<RelFieldCollation> liftedFcs = new ArrayList<>(sortExprs.size());
        List<RelFieldCollation> origFcs = sort.getCollation().getFieldCollations();
        for (int i = 0; i < sortExprs.size(); i++) {
            RelFieldCollation orig = origFcs.get(i);
            liftedFcs.add(new RelFieldCollation(origColCount + i, orig.direction, orig.nullDirection));
        }
        RelCollation liftedColl = RelCollations.of(liftedFcs);
        RelNode sortRel = LogicalSort.create(lift, liftedColl, sort.offset, sort.fetch);

        // Drop Project: project away the lifted columns, restore original schema.
        List<RexNode> dropExprs = new ArrayList<>(origColCount);
        List<String> dropNames = new ArrayList<>(origColCount);
        for (int i = 0; i < origColCount; i++) {
            RelDataTypeField f = child.getRowType().getFieldList().get(i);
            dropExprs.add(rb.makeInputRef(f.getType(), i));
            dropNames.add(f.getName());
        }
        return LogicalProject.create(sortRel, List.of(), dropExprs, dropNames, Set.of());
    }

    private static String uniqueName(String desired, Set<String> used) {
        if (used.add(desired)) return desired;
        int n = 1;
        while (true) {
            String candidate = desired + "_" + n;
            if (used.add(candidate)) return candidate;
            n++;
        }
    }
}
