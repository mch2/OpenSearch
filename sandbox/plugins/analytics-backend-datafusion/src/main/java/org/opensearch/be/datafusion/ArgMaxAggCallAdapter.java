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
 * Rewrites {@code ARG_MAX(value, key)} to {@code LAST_VALUE(value) ORDER BY key ASC NULLS LAST}.
 * PPL {@code latest(field, ts)} lowers to Calcite's {@code ARG_MAX(field, ts)} at the frontend;
 * DataFusion 52.x has no native {@code max_by}/{@code arg_max} UDAF. The semantic equivalent is
 * {@code last_value(field)} with an {@code ORDER BY} on the key arg — the last row of an
 * ascending sort is the row with the largest key.
 *
 * @opensearch.internal
 */
class ArgMaxAggCallAdapter implements UnaryOperator<AggregateCall> {

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
            SqlStdOperatorTable.LAST_VALUE,
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
