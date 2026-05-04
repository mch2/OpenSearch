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
 * Rewrites {@code ARRAY_COMPACT(list<T>)} → {@code array_remove_all(list<T>, CAST(NULL AS T))}.
 * <p>
 * DataFusion 52.5.0 has no {@code array_compact} built-in but does expose
 * {@code array_remove_all(array, element)}, which removes every occurrence of
 * {@code element} from {@code array}. Passing a typed NULL for {@code element}
 * reproduces {@code array_compact}'s null-stripping semantics.
 * <p>
 * The NULL literal is cast to the array's element type so that Substrait's
 * signature resolver matches the {@code array_remove_all(list<any1>, any1)}
 * variant declared in {@code opensearch_scalar.yaml}.
 *
 * @opensearch.internal
 */
class ArrayCompactAdapter implements ScalarFunctionAdapter {

    private static final SqlFunction ARRAY_REMOVE_ALL = new SqlFunction(
        "array_remove_all",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            throw new IllegalArgumentException("ARRAY_COMPACT expects 1 operand, got " + original.getOperands().size());
        }
        RexNode array = original.getOperands().get(0);
        RelDataType arrayType = array.getType();
        if (arrayType.getSqlTypeName() != SqlTypeName.ARRAY && arrayType.getSqlTypeName() != SqlTypeName.MULTISET) {
            throw new IllegalArgumentException(
                "ARRAY_COMPACT expects an array/multiset operand, got " + arrayType.getSqlTypeName()
            );
        }
        RelDataType elementType = arrayType.getComponentType();
        if (elementType == null) {
            throw new IllegalArgumentException("ARRAY_COMPACT operand has no component type: " + arrayType);
        }
        RexBuilder rb = cluster.getRexBuilder();
        // Typed NULL matches the element type so the substrait signature resolver picks
        // the array_remove_all(list<any1>, any1) variant with any1 bound to elementType.
        RexNode typedNull = rb.makeNullLiteral(elementType);
        return rb.makeCall(original.getType(), ARRAY_REMOVE_ALL, List.of(array, typedNull));
    }
}
