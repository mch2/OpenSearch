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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Cat-3b adapter for PPL's polymorphic {@code SPAN(field, interval, unit)}.
 * Rewrites the call to a locally-declared {@code span} SqlFunction (which
 * resolves via substrait to the {@code span} Rust UDF) AND normalizes the
 * {@code unit} operand to a typed literal that isthmus's substrait
 * serializer can handle.
 *
 * <p><b>Why not pure rename (cat 3a via {@link AbstractNameMappingAdapter})?</b>
 * PPL's {@code CalciteRexNodeVisitor.visitSpan} emits
 * {@code SPAN(field, interval, rexBuilder.constantNull())} for numeric spans.
 * The resulting null literal has Calcite {@link SqlTypeName#NULL} — and
 * isthmus's {@code TypeConverter.toSubstrait} has no branch for
 * {@code NULL} type, so fragment conversion throws
 * {@code "Unable to convert the type NULL"}. A pure-rename adapter
 * preserves the untyped null and hits that wall at substrait-ify time.
 *
 * <p>This adapter substitutes the untyped-null unit operand with a
 * {@code VARCHAR NULL} (typed null literal). Isthmus can serialize
 * {@code VARCHAR NULL}; DataFusion's substrait consumer materialises it as
 * a {@code NullArray} (or {@code Null(Utf8)} literal) which the Rust UDF's
 * {@code ensure_numeric_mode} treats as numeric-mode. Any non-null string
 * literal flows through unchanged — the Rust UDF returns a plan error if a
 * time unit reaches the data node (coord-side bridge expected for date/time
 * span).
 *
 * <p><b>Scope:</b> numeric-span branch only. Date/time span bridged
 * coordinator-side per the three-tier execution pattern documented on the
 * {@code span} Rust UDF.
 *
 * <p><b>Semantics note — floor vs truncation:</b> DataFusion-backed SPAN on
 * an integer column uses floor semantics after Float64 coercion (matches
 * DataFusion's native numerics and subtraitupdates's opensearch_span
 * kernel). PPL's Java-local execution uses integer truncation (towards
 * zero). The two diverge only on negative integer values with non-factor
 * intervals (e.g. SPAN(-5, 3): Java-local yields -3; DF-backend yields -6).
 *
 * @opensearch.internal
 */
class SpanAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. {@link SqlKind#OTHER_FUNCTION} to
     * avoid Calcite built-in collisions. Operand-type checker permissive on
     * the unit slot since numeric-span callers pass VARCHAR NULL (Calcite
     * null literal typed as VARCHAR).
     */
    static final SqlOperator LOCAL_SPAN_OP = new SqlFunction(
        "span",
        SqlKind.OTHER_FUNCTION,
        // Placeholder inference; the adapter clones with the original's
        // RelDataType so this doesn't drive actual type resolution.
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = new ArrayList<>(original.getOperands());
        // Retype a SqlTypeName.NULL unit operand to VARCHAR-nullable. The raw
        // untyped null is emitted by CalciteRexNodeVisitor.visitSpan for
        // numeric spans and isthmus can't serialize it. VARCHAR-nullable is
        // accepted by the substrait serializer and the Rust UDF's
        // ensure_numeric_mode treats null-of-any-string-type as numeric-mode.
        if (operands.size() == 3 && operands.get(2).getType().getSqlTypeName() == SqlTypeName.NULL) {
            RelDataType varcharNullable = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
            operands.set(2, rexBuilder.makeNullLiteral(varcharNullable));
        }
        // Preserve the original call's return type (same regression guard as
        // AbstractNameMappingAdapter). The enclosing Project caches rowType
        // from the pre-adaptation expression; any drift breaks Project.isValid.
        return rexBuilder.makeCall(original.getType(), LOCAL_SPAN_OP, operands);
    }
}
