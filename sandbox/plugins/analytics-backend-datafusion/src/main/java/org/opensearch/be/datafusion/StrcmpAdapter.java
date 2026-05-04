/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites {@code strcmp(a, b)} → {@code CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END}.
 * <p>
 * PPL emits {@code strcmp} as a Calcite {@code SqlLibraryOperators.STRCMP} call (with operands
 * already swapped so PPL's {@code strcmp(left, right)} matches MySQL semantics). DataFusion has no
 * equivalent built-in, but the comparison fold is straightforward — three-way compare on
 * lexicographic order.
 *
 * @opensearch.internal
 */
class StrcmpAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("STRCMP expects 2 operands, got " + original.getOperands().size());
        }
        RexBuilder builder = cluster.getRexBuilder();
        RexNode a = original.getOperands().get(0);
        RexNode b = original.getOperands().get(1);
        // CASE branches must have matching nullability; the outer row type derives from
        // the original STRCMP result. Match that: use the SqlLibraryOperators.STRCMP
        // return-type nullability (nullable iff any operand is nullable) for the literals.
        RelDataType intType = original.getType();
        RexNode minusOne = builder.makeCast(intType, builder.makeExactLiteral(BigDecimal.valueOf(-1)));
        RexNode zero = builder.makeCast(intType, builder.makeExactLiteral(BigDecimal.ZERO));
        RexNode one = builder.makeCast(intType, builder.makeExactLiteral(BigDecimal.ONE));
        return builder.makeCall(
            intType,
            SqlStdOperatorTable.CASE,
            List.of(
                builder.makeCall(SqlStdOperatorTable.LESS_THAN, a, b),
                minusOne,
                builder.makeCall(SqlStdOperatorTable.GREATER_THAN, a, b),
                one,
                zero));
    }
}
