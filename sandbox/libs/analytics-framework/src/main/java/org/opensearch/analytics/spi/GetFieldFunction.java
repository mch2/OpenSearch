/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * {@code get_field(struct, 'name')} → the value of one field of a struct.
 *
 * <p>The counterpart to storing an OpenSearch {@code object} as a native Parquet struct: the scan
 * produces the object as one column, while the schema still exposes each leaf under its dotted name
 * because that is how a query addresses it.
 * A projection above the scan reconstructs those leaf columns by reaching into the struct, nesting
 * the call for a sub-object:
 *
 * <pre>
 * get_field(get_field($1, 'properties'), 'name')      // the column named city.properties.name
 * </pre>
 *
 * <p>Calcite's own {@code RexFieldAccess} says the same thing more directly, but it does not survive
 * the trip: isthmus serializes it to a nested Substrait field reference, and DataFusion's Substrait
 * consumer rejects those with "Direct reference StructField with child is not supported". A function
 * invocation goes through, so that is the form used.
 *
 * <p>The return type is always supplied by the caller via {@link #makeCall}: it is the leaf's type
 * from the index mapping, which no amount of operand inspection would yield, so the operand-driven
 * inference on the operator is a placeholder.
 *
 * @opensearch.internal
 */
public final class GetFieldFunction {

    /** The function name used in Calcite plans and Substrait serialization. */
    public static final String NAME = "get_field";

    /** Singleton Calcite SqlFunction: {@code get_field(ANY, VARCHAR) → ANY}. */
    public static final SqlFunction FUNCTION = new SqlFunction(
        NAME,
        SqlKind.OTHER_FUNCTION,
        opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY),
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private GetFieldFunction() {}

    /**
     * Builds {@code get_field(struct, 'fieldName')} with an explicit return type.
     *
     * @param rexBuilder builder for the enclosing plan
     * @param fieldType  the type of the field being read (from the index mapping)
     * @param struct     the struct-valued expression to read from
     * @param fieldName  the name of the field within {@code struct}
     */
    public static RexNode makeCall(RexBuilder rexBuilder, RelDataType fieldType, RexNode struct, String fieldName) {
        // VARCHAR, not CHAR: makeLiteral(String) yields CHAR(n), whose padding semantics are wrong
        // for a field name, and the backend depends on the distinction.
        RelDataType nameType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        return rexBuilder.makeCall(fieldType, FUNCTION, List.of(struct, rexBuilder.makeLiteral(fieldName, nameType, true)));
    }
}
