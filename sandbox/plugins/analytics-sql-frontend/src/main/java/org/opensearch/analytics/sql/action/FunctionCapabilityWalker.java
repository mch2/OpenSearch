/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WindowFunction;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Fail-closed guard (R2/R3): walks a logical {@link RelNode} and rejects it if it references any
 * function that validated against Calcite's standard operator table but has no Analytics Engine
 * implementation. Resolution goes through AE's <em>real</em> resolution path —
 * {@link ScalarFunction#fromSqlOperatorWithFallback}, {@link AggregateFunction#fromSqlAggFunction},
 * {@link WindowFunction#resolveFunction} — the same seams AE's own marking rules use, so this
 * cannot drift from what AE actually executes (no parallel allowlist).
 *
 * <p>An operator that AE cannot resolve (scalar → {@code null}; aggregate/window → the resolver
 * throws) is collected; a non-empty set throws {@link UnsupportedFunctionException} naming the
 * offenders. The walk runs after convert and before execution, so nothing unsupported ever
 * reaches the executor.
 */
final class FunctionCapabilityWalker {

    private FunctionCapabilityWalker() {}

    /** Throws {@link UnsupportedFunctionException} if {@code plan} references an AE-unsupported function. */
    static void check(RelNode plan) {
        Set<String> unsupported = new LinkedHashSet<>();

        // Visits a single node's own Rex expressions in place. RexOver is a RexCall, so visitCall
        // catches window ops too. We never mutate — this is a read-only capability scan.
        RexShuttle rexShuttle = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                // Window functions surface as RexOver, whose agg operator resolves through the
                // WindowFunction path (defensive coverage — v1's grammar does not commit to window
                // functions, but a windowed query must not slip past).
                if (call instanceof RexOver over) {
                    SqlAggFunction windowOp = over.getAggOperator();
                    if (WindowFunction.resolveFunction(windowOp) == null) {
                        unsupported.add(windowOp.getName());
                    }
                } else if (ScalarFunction.fromSqlOperatorWithFallback(call.getOperator()) == null) {
                    unsupported.add(operatorLabel(call.getOperator()));
                }
                return super.visitCall(call);
            }
        };

        // Explicit recursion over the tree. RelShuttleImpl has TYPED visit(LogicalProject/Aggregate/
        // …) overloads that bypass a generic visit(RelNode) override, so a shuttle would silently
        // skip exactly the nodes carrying expressions. Walking getInputs() ourselves visits every
        // node regardless of its concrete class.
        walk(plan, rexShuttle, unsupported);

        if (unsupported.isEmpty() == false) {
            throw new UnsupportedFunctionException(unsupported);
        }
    }

    private static void walk(RelNode node, RexShuttle rexShuttle, Set<String> unsupported) {
        node.accept(rexShuttle);
        if (node instanceof Aggregate aggregate) {
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                checkAggregate(aggCall.getAggregation(), unsupported);
            }
        }
        for (RelNode input : node.getInputs()) {
            walk(input, rexShuttle, unsupported);
        }
    }

    private static void checkAggregate(SqlAggFunction aggregation, Set<String> unsupported) {
        try {
            AggregateFunction.fromSqlAggFunction(aggregation);
        } catch (IllegalStateException noMapping) {
            unsupported.add(aggregation.getName());
        }
    }

    private static String operatorLabel(SqlOperator operator) {
        String name = operator.getName();
        return (name == null || name.isEmpty()) ? operator.getKind().toString() : name;
    }
}
