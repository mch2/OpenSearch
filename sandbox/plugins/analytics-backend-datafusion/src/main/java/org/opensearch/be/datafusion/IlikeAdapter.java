/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code ILIKE(input, pattern[, escape])} → {@code LIKE(LOWER(input), LOWER(pattern))}.
 * <p>
 * Substrait core doesn't define an {@code ilike} variant, and DataFusion's
 * substrait consumer only recognizes a case-sensitive {@code like}. Lowercasing
 * both sides reduces to the case-insensitive comparison Calcite's {@code ILIKE}
 * promises. The optional escape operand is dropped — same as
 * {@link LikeEscapeTransformer} does for {@code LIKE}.
 *
 * @opensearch.internal
 */
class IlikeAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() < 2) {
            throw new IllegalArgumentException("ILIKE expects 2-3 operands, got " + original.getOperands().size());
        }
        RexBuilder builder = cluster.getRexBuilder();
        RexNode input = original.getOperands().get(0);
        RexNode pattern = original.getOperands().get(1);
        RexNode lowerInput = builder.makeCall(SqlStdOperatorTable.LOWER, input);
        RexNode lowerPattern = builder.makeCall(SqlStdOperatorTable.LOWER, pattern);
        return builder.makeCall(original.getType(), SqlStdOperatorTable.LIKE, List.of(lowerInput, lowerPattern));
    }
}
