/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-substrait rewrite that eliminates untyped {@code NULL} literals from
 * Calcite {@link RexNode} trees.
 *
 * <p>Context: the PPL trendline visitor (SQL plugin) emits
 * {@code CASE(cond, then, null:NULL)} — the {@code ELSE} branch uses an untyped
 * null literal (Calcite {@link SqlTypeName#NULL}). Isthmus's
 * {@code LiteralConverter} rejects that type with
 * {@code "Unable to convert the type NULL"}. Since {@code CASE} in SQL requires
 * every branch to be castable to the result type, we can safely replace each
 * such literal with a typed-null whose type equals the enclosing {@code RexCall}'s
 * result type.
 *
 * <p>The same pattern can appear in any PPL visitor that emits an untyped null
 * as a default branch; rewriting at the convertor boundary keeps the fix
 * downstream of all such emission sites without having to touch the SQL plugin.
 *
 * @opensearch.internal
 */
final class UntypedNullRewriter {

    private UntypedNullRewriter() {}

    /** Rewrites every RexNode reachable from {@code root}, returning a new tree if any change was made. */
    static RelNode rewrite(RelNode root) {
        return root.accept(new Shuttle());
    }

    /** RelShuttle that applies {@link RexFixer} to every expression-bearing RelNode. */
    private static final class Shuttle extends RelShuttleImpl {
        @Override
        public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
            return visited.accept(new RexFixer(rexBuilder));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalProject project) {
            return fix(super.visit(project));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalFilter filter) {
            return fix(super.visit(filter));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalJoin join) {
            return fix(super.visit(join));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalAggregate aggregate) {
            return fix(super.visit(aggregate));
        }

        @Override
        public RelNode visit(org.apache.calcite.rel.logical.LogicalSort sort) {
            return fix(super.visit(sort));
        }

        private RelNode fix(RelNode node) {
            // Cover core.* subclasses too (e.g. Project / Filter / Join / Sort / Aggregate produced
            // by non-logical rules). Generic accept() is enough because RexShuttle handles all
            // RexNode-bearing surfaces (projects, conditions, aggCalls' filter exprs, etc.).
            if (node instanceof Project || node instanceof Filter || node instanceof Join
                || node instanceof Aggregate || node instanceof Sort) {
                RexBuilder rexBuilder = node.getCluster().getRexBuilder();
                return node.accept(new RexFixer(rexBuilder));
            }
            return node;
        }
    }

    /**
     * RexShuttle that replaces every {@code RexLiteral(null, NULL)} appearing
     * as an operand of a typed {@code RexCall} with a typed null whose type
     * matches the enclosing call's return type.
     */
    private static final class RexFixer extends RexShuttle {
        private final RexBuilder rexBuilder;

        RexFixer(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<RexNode> oldOperands = call.getOperands();
            RelDataType callType = call.getType();
            List<RexNode> newOperands = new ArrayList<>(oldOperands.size());
            boolean changed = false;
            for (RexNode op : oldOperands) {
                RexNode visited = op.accept(this);
                if (isUntypedNull(visited) && callType.getSqlTypeName() != SqlTypeName.NULL) {
                    // Typed-null whose type matches the enclosing call — CASE requires each
                    // branch to be castable to the result type, so this preserves semantics.
                    newOperands.add(rexBuilder.makeNullLiteral(callType));
                    changed = true;
                } else {
                    newOperands.add(visited);
                    if (visited != op) {
                        changed = true;
                    }
                }
            }
            if (changed) {
                return call.clone(call.getType(), newOperands);
            }
            return call;
        }

        private static boolean isUntypedNull(RexNode node) {
            return node instanceof RexLiteral lit
                && lit.isNull()
                && lit.getType().getSqlTypeName() == SqlTypeName.NULL;
        }
    }
}
