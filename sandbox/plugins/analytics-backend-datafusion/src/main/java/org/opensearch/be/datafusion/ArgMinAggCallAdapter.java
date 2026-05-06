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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Rewrites {@code ARG_MIN(value, key)} to {@code FIRST_VALUE(value) ORDER BY key ASC NULLS LAST}.
 * PPL {@code earliest(field, ts)} lowers to Calcite's {@code ARG_MIN(field, ts)} at the frontend;
 * DataFusion 52.x has no native {@code min_by}/{@code arg_min} UDAF. The semantic equivalent is
 * {@code first_value(field)} with an {@code ORDER BY} on the key arg.
 *
 * <p>Isthmus's stock {@link io.substrait.isthmus.expression.AggregateFunctionConverter} reads
 * {@link AggregateCall#getCollation()} and emits it as an {@code ORDER BY} sort field on the
 * resulting substrait invocation, so pulling the key out of the arg list into the collation is
 * the only transformation needed here.
 *
 * @opensearch.internal
 */
class ArgMinAggCallAdapter implements UnaryOperator<AggregateCall> {

    @Override
    public AggregateCall apply(AggregateCall original) {
        if (original.getArgList().size() != 2) {
            return original;
        }
        int valueIdx = original.getArgList().get(0);
        int keyIdx = original.getArgList().get(1);
        RelCollation collation = RelCollations.of(
            new RelFieldCollation(keyIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)
        );
        return AggregateCall.create(
            SqlStdOperatorTable.FIRST_VALUE,
            original.isDistinct(),
            original.isApproximate(),
            original.ignoreNulls(),
            List.of(valueIdx),
            original.filterArg,
            collation,
            original.getType(),
            original.getName()
        );
    }
}
