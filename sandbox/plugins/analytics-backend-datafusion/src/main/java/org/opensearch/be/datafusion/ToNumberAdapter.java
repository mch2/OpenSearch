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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code tonumber(x)} → {@code CAST(x AS DOUBLE)}. PPL emits
 * {@code tonumber(...)} as a UDF call; DataFusion has no native equivalent —
 * but the semantics map directly to a CAST. Doing the rewrite at the
 * BackendPlanAdapter stage keeps the PPL frontend backend-agnostic.
 *
 * @opensearch.internal
 */
class ToNumberAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("TONUMBER expects 1 operand, got " + original.getOperands().size());
        }
        RexNode operand = original.getOperands().get(0);
        RelDataType doubleType = cluster.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        RelDataType target = operand.getType().isNullable()
            ? cluster.getTypeFactory().createTypeWithNullability(doubleType, true)
            : doubleType;
        return cluster.getRexBuilder().makeCast(target, operand);
    }
}
