/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.be.datafusion.planner.adapter.NumericConversionFunctionAdapter;
import org.opensearch.be.datafusion.planner.adapter.TimeConversionFunctionAdapter;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;
import java.util.Optional;

import io.substrait.isthmus.expression.FunctionMappings;

/**
 * Local function stubs and their Substrait signature bindings for the DataFusion backend.
 *
 * <p>These are the operators {@link DataFusionFragmentConvertor} hands to isthmus so PPL functions
 * bind to DataFusion's extension URNs: scalar accessor stubs ({@code pattern_parser_get_*}), the
 * state-expanding aggregate stubs ({@code take}/{@code first_value}/{@code array_agg}/…) that
 * {@link PplAggregateCallRewriter} swaps in, and the {@code ADDITIONAL_*_SIGS} sig lists. Kept out of
 * the converter so it holds only conversion logic; the per-function scalar ops continue to live in
 * their own {@code *Adapter} classes and are merely referenced from the sig lists here.
 */
final class LocalFunctionOps {

    private LocalFunctionOps() {}

    /** Per-field accessors for {@code pattern_parser}'s STRUCT output; see {@link ItemTypeRebuilder}. */
    static final SqlOperator LOCAL_PATTERN_PARSER_GET_PATTERN_OP = new SqlFunction(
        "pattern_parser_get_pattern",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    static final SqlOperator LOCAL_PATTERN_PARSER_GET_TOKENS_OP = new SqlFunction(
        "pattern_parser_get_tokens",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Local stubs for PPL state-expanding aggregates; swapped in by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_TAKE_OP = new SqlAggFunction(
        "take",
        null,
        SqlKind.OTHER_FUNCTION,
        // FORCE_NULLABLE so AggregateCall.create accepts a nullable explicit return type.
        ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_FIRST_OP = new SqlAggFunction(
        "first_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_LAST_OP = new SqlAggFunction(
        "last_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * LIST/VALUES — carries the PPL element-rendering contract via the {@link LocalAggOp} hooks:
     * cast to VARCHAR (lowercase booleans), drop nulls, and (VALUES only) lexicographic sort.
     * Inferred return type is {@code ARRAY<VARCHAR>}.
     */
    static final SqlAggFunction LOCAL_ARRAY_AGG_OP = new LocalAggOp("array_agg", SqlKind.OTHER_FUNCTION, opBinding -> {
        RelDataTypeFactory tf = opBinding.getTypeFactory();
        return tf.createTypeWithNullability(tf.createArrayType(tf.createSqlType(SqlTypeName.VARCHAR), -1), true);
    }, OperandTypes.ANY) {
        @Override
        public Optional<RexNode> rewriteDataArg(int argIndex, RexNode argRef, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            // Skip array operands (partial→final merge path) and already-VARCHAR operands.
            if (argRef.getType().getComponentType() != null || argRef.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                return Optional.empty();
            }
            return Optional.of(castToVarchar(argRef, rexBuilder, typeFactory));
        }

        @Override
        public boolean sortsArgAscending(AggregateCall call) {
            // VALUES (isDistinct) returns lexicographically sorted distinct strings; LIST does not sort.
            return call.isDistinct();
        }

        @Override
        public boolean filtersNullArgs(AggregateCall call) {
            // list/values drop null elements per the PPL contract.
            return true;
        }
    };

    /**
     * Casts a list/values element to VARCHAR matching the SQL plugin's {@code String.valueOf}
     * rendering: ip→{@code ip_to_string}, binary→{@code binary_to_base64}, else a plain CAST.
     * Unlike the {@code cast}/{@code tostring} path this does NOT uppercase booleans — native
     * {@code cast(boolean AS Utf8)} yields lowercase {@code true}/{@code false}, per the PPL
     * {@code list}/{@code values} contract.
     */
    private static RexNode castToVarchar(RexNode arg, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        if (arg.getType() instanceof org.opensearch.analytics.schema.IpType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, List.of(arg));
        }
        if (arg.getType() instanceof org.opensearch.analytics.schema.BinaryType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, List.of(arg));
        }
        return rexBuilder.makeCast(varcharNullable, arg);
    }

    /** FINAL-side merge for LIST; un-nests per-shard list states. */
    static final SqlAggFunction LOCAL_LIST_MERGE_OP = new SqlAggFunction(
        "list_merge",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** FINAL-side merge for VALUES — re-deduplicates after concatenation. */
    static final SqlAggFunction LOCAL_LIST_MERGE_DISTINCT_OP = new SqlAggFunction(
        "list_merge_distinct",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * PPL {@code percentile_approx(field, percentile)} → DataFusion's builtin
     * {@code approx_percentile_cont(field, percentile)}. PPL's trailing field-type-flag
     * arg is stripped by {@link PplAggregateCallRewriter} before binding; the percentile
     * literal is rescaled from PPL's [0, 100] to DataFusion's [0, 1] convention via
     * {@link LocalAggOp#normaliseLiteralArg} at substrait emission.
     */
    static final LocalAggOp LOCAL_PERCENTILE_APPROX_OP = new LocalAggOp(
        "approx_percentile_cont",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        OperandTypes.ANY_ANY
    ) {
        @Override
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            if (argIndex == 1 && lit.getValue() instanceof BigDecimal bd) {
                BigDecimal scaled = bd.divide(BigDecimal.valueOf(100), MathContext.DECIMAL64);
                RelDataType doubleType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
                return rexBuilder.makeLiteral(scaled, doubleType);
            }
            return lit;
        }
    };

    /** BRAIN window stub for {@code patterns ... method=BRAIN mode=label}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_WINDOW_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** BRAIN aggregate stub; return type is supplied by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final List<FunctionMappings.Sig> ADDITIONAL_SCALAR_SIGS = List.of(
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(AggregateFunction.REDUCE_EVAL_OP, "reduce_eval"),
        FunctionMappings.s(DelegationPossibleFunction.FUNCTION, DelegationPossibleFunction.NAME),
        FunctionMappings.s(SqlStdOperatorTable.ASCII, "ascii"),
        FunctionMappings.s(SqlStdOperatorTable.CHAR_LENGTH, "length"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_FUNCTION, "concat"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_WS, "concat_ws"),
        FunctionMappings.s(SqlLibraryOperators.ILIKE, "ilike"),
        FunctionMappings.s(SqlLibraryOperators.DATE_PART, "date_part"),
        FunctionMappings.s(SqlLibraryOperators.TO_CHAR, "to_char"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, "ip_to_string"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, "binary_to_base64"),
        FunctionMappings.s(SqlLibraryOperators.DATE_TRUNC, "date_trunc"),
        FunctionMappings.s(SpanAdapter.LOCAL_DATE_BIN_OP, "date_bin"),
        FunctionMappings.s(PatternParserAdapter.LOCAL_PATTERN_PARSER_OP, "pattern_parser"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_PATTERN_OP, "pattern_parser_get_pattern"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_TOKENS_OP, "pattern_parser_get_tokens"),
        FunctionMappings.s(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, "convert_tz"),
        FunctionMappings.s(ParseAdapter.LOCAL_PARSE_OP, "parse"),
        FunctionMappings.s(GrokAdapter.LOCAL_GROK_OP, "grok"),
        FunctionMappings.s(SqlStdOperatorTable.ITEM, "item"),
        FunctionMappings.s(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, "to_unixtime"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_NOW_OP, "now"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_DATE_OP, "current_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_TIME_OP, "current_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TIME_OP, "to_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_OP, "to_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, "to_timestamp"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_TRUNC_OP, "date_trunc"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, "opensearch_extract"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, "from_unixtime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKEDATE_OP, "makedate"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP, "maketime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, "date_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, "time_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_STR_TO_DATE_OP, "str_to_date"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_OS_WEEK_OP, "os_week"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_PG_4, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REVERSE, "reverse"),
        FunctionMappings.s(SqlLibraryOperators.TRANSLATE3, "translate"),
        FunctionMappings.s(PositionAdapter.STRPOS, "strpos"),
        FunctionMappings.s(StrftimeFunctionAdapter.STRFTIME, "strftime"),
        FunctionMappings.s(ToNumberFunctionAdapter.TONUMBER, "tonumber"),
        FunctionMappings.s(ToStringFunctionAdapter.TOSTRING, "tostring"),
        FunctionMappings.s(SqlLibraryOperators.MD5, "md5"),
        FunctionMappings.s(SqlLibraryOperators.SHA1, "sha1"),
        FunctionMappings.s(SqlLibraryOperators.CRC32, "crc32"),
        FunctionMappings.s(Sha2FunctionAdapter.DIGEST, "digest"),
        FunctionMappings.s(Sha2FunctionAdapter.ENCODE, "encode"),
        FunctionMappings.s(RexExtractAdapter.LOCAL_REX_EXTRACT_OP, "rex_extract"),
        FunctionMappings.s(RexExtractMultiAdapter.LOCAL_REX_EXTRACT_MULTI_OP, "rex_extract_multi"),
        FunctionMappings.s(RexOffsetAdapter.LOCAL_REX_OFFSET_OP, "rex_offset"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(NumericConversionFunctionAdapter.NUM, "num"),
        FunctionMappings.s(NumericConversionFunctionAdapter.AUTO, "auto"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MEMK, "memk"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMCOMMA, "rmcomma"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMUNIT, "rmunit"),
        FunctionMappings.s(NumericConversionFunctionAdapter.DUR2SEC, "dur2sec"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MSTIME, "mstime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.CTIME, "ctime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.MKTIME, "mktime"),
        FunctionMappings.s(SqlStdOperatorTable.TRUNCATE, "trunc"),
        FunctionMappings.s(SqlStdOperatorTable.CBRT, "cbrt"),
        FunctionMappings.s(SqlStdOperatorTable.COT, "cot"),
        FunctionMappings.s(SqlStdOperatorTable.PI, "pi"),
        FunctionMappings.s(SqlStdOperatorTable.RAND, "random"),
        FunctionMappings.s(SqlLibraryOperators.LOG, "logb"),
        FunctionMappings.s(SignumFunction.FUNCTION, SignumFunction.NAME),
        FunctionMappings.s(JsonFunctionAdapters.JsonAppendAdapter.LOCAL_JSON_APPEND_OP, "json_append"),
        FunctionMappings.s(JsonFunctionAdapters.JsonArrayLengthAdapter.LOCAL_JSON_ARRAY_LENGTH_OP, "json_array_length"),
        FunctionMappings.s(JsonFunctionAdapters.JsonDeleteAdapter.LOCAL_JSON_DELETE_OP, "json_delete"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtendAdapter.LOCAL_JSON_EXTEND_OP, "json_extend"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAdapter.LOCAL_JSON_EXTRACT_OP, "json_extract"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAllAdapter.LOCAL_JSON_EXTRACT_ALL_OP, "json_extract_all"),
        FunctionMappings.s(JsonFunctionAdapters.JsonKeysAdapter.LOCAL_JSON_KEYS_OP, "json_keys"),
        FunctionMappings.s(JsonFunctionAdapters.JsonSetAdapter.LOCAL_JSON_SET_OP, "json_set"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_SLICE, "array_slice"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_DISTINCT, "array_distinct"),
        FunctionMappings.s(MakeArrayAdapter.LOCAL_MAKE_ARRAY_OP, "make_array"),
        FunctionMappings.s(ArrayToStringAdapter.LOCAL_ARRAY_TO_STRING_OP, "array_to_string"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_ARRAY_ELEMENT_OP, "array_element"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_MAP_EXTRACT_OP, "map_extract"),
        FunctionMappings.s(MvzipAdapter.LOCAL_MVZIP_OP, "mvzip"),
        FunctionMappings.s(MvfindAdapter.LOCAL_MVFIND_OP, "mvfind"),
        FunctionMappings.s(MvappendAdapter.LOCAL_MVAPPEND_OP, "mvappend"),
        FunctionMappings.s(SpanBucketAdapter.LOCAL_SPAN_BUCKET_OP, "span_bucket"),
        FunctionMappings.s(WidthBucketAdapter.LOCAL_WIDTH_BUCKET_OP, "width_bucket"),
        FunctionMappings.s(MinspanBucketAdapter.LOCAL_MINSPAN_BUCKET_OP, "minspan_bucket"),
        FunctionMappings.s(RangeBucketAdapter.LOCAL_RANGE_BUCKET_OP, "range_bucket"),
        FunctionMappings.s(ConvAdapter.LOCAL_CONV_OP, "conv")
    );

    static final List<FunctionMappings.Sig> ADDITIONAL_AGGREGATE_SIGS = List.of(
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct"),
        FunctionMappings.s(LOCAL_TAKE_OP, "take"),
        FunctionMappings.s(LOCAL_FIRST_OP, "first_value"),
        FunctionMappings.s(LOCAL_LAST_OP, "last_value"),
        FunctionMappings.s(LOCAL_ARRAY_AGG_OP, "array_agg"),
        FunctionMappings.s(LOCAL_LIST_MERGE_OP, "list_merge"),
        FunctionMappings.s(LOCAL_LIST_MERGE_DISTINCT_OP, "list_merge_distinct"),
        FunctionMappings.s(LOCAL_PERCENTILE_APPROX_OP, "approx_percentile_cont"),
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_OP, "internal_pattern")
    );

    static final List<FunctionMappings.Sig> ADDITIONAL_WINDOW_SIGS = List.of(
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_WINDOW_OP, "internal_pattern"),
        // Mirror ADDITIONAL_AGGREGATE_SIGS: rename APPROX_COUNT_DISTINCT to DataFusion's `approx_distinct`.
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct")
    );

    /**
     * Local aggregate stub that may transform inlined literal args before substrait emission.
     * Other local stubs without transformations stay as plain {@link SqlAggFunction}; the
     * {@code convert()} override only invokes {@link #normaliseLiteralArg} when the call's
     * operator is a {@code LocalAggOp}, so adding a new normalisation is purely a matter of
     * subclassing here next to the op's declaration.
     */
    abstract static class LocalAggOp extends SqlAggFunction {
        LocalAggOp(
            String name,
            SqlKind kind,
            org.apache.calcite.sql.type.SqlReturnTypeInference returnTypeInference,
            org.apache.calcite.sql.type.SqlOperandTypeChecker operandTypeChecker
        ) {
            super(
                name,
                null,
                kind,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false,
                Optionality.FORBIDDEN
            );
        }

        /** Identity by default; override to transform the {@code argIndex}-th inlined literal arg. */
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            return lit;
        }

        /**
         * Returns the expression to emit for the {@code argIndex}-th data arg in place of a bare
         * field reference (e.g. a type-coercing CAST), or empty to keep the reference. Applied on
         * the bound Substrait argument, so it rides the measure without a child Project that the
         * reduce-stage stitch ({@code replaceInput}) would drop. Identity by default.
         */
        public Optional<RexNode> rewriteDataArg(int argIndex, RexNode argRef, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            return Optional.empty();
        }

        /**
         * Whether the aggregate's elements are emitted ascending-sorted by the (rewritten) data arg.
         * Carried as the invocation's sort, which DataFusion's {@code array_agg} honours. False by default.
         */
        public boolean sortsArgAscending(AggregateCall call) {
            return false;
        }

        /**
         * Whether null arguments are dropped before aggregating. Carried as the measure's
         * {@code is_not_null} preMeasureFilter (DataFusion's substrait consumer can't take the
         * function's own {@code ignore_nulls}). False by default.
         */
        public boolean filtersNullArgs(AggregateCall call) {
            return false;
        }
    }
}
