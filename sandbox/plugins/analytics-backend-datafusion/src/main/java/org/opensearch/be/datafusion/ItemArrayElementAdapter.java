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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites {@code ITEM($array, $index)} to {@code array_element($array, $index)} when
 * the first operand is an array/list type. DataFusion's substrait consumer only
 * honours {@code ITEM} for struct field access (StructField direct reference) and
 * rejects array indexing with "Direct reference with types other than StructField
 * is not supported". {@code array_element} is a DataFusion built-in.
 *
 * <p>Struct-typed {@code ITEM} calls are passed through unchanged — DataFusion
 * handles those natively via its StructField reference path.
 *
 * <p>PPL emits this via {@code mvindex(array, index)} which lowers through
 * {@code MVIndexFunctionImp#resolveSingleElement} to Calcite's
 * {@code SqlStdOperatorTable.ITEM} with a 1-based normalized index.
 * {@code array_element} uses the same 1-based indexing semantics, so no
 * index rewrite is needed.
 *
 * @opensearch.internal
 */
class ItemArrayElementAdapter implements ScalarFunctionAdapter {

    private static final SqlFunction ARRAY_ELEMENT = new SqlFunction(
        "array_element",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            throw new IllegalArgumentException("ITEM expects 2 operands, got " + original.getOperands().size());
        }
        RexNode container = original.getOperands().get(0);
        SqlTypeName containerTypeName = container.getType().getSqlTypeName();
        // Only rewrite the array/multiset indexing variant. Struct field access is a direct
        // StructField reference in DataFusion's substrait consumer and must pass through.
        if (containerTypeName != SqlTypeName.ARRAY && containerTypeName != SqlTypeName.MULTISET) {
            return original;
        }
        RelDataType returnType = original.getType();
        return cluster.getRexBuilder().makeCall(
            returnType,
            ARRAY_ELEMENT,
            List.of(container, original.getOperands().get(1))
        );
    }
}
