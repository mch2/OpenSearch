/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.substrait.isthmus.expression;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.TypeCreator;

/**
 * Unit tests for {@link NameBasedScalarFunctionConverter} — the YAML-driven alias +
 * literal-arg-injection layer that translates Calcite {@link RexCall} instances to
 * Substrait scalar function invocations the DataFusion backend can resolve.
 */
public class NameBasedScalarFunctionConverterTests extends OpenSearchTestCase {

    private static final TypeCreator N = TypeCreator.of(true);

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(NameBasedScalarFunctionConverterTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            collection = mergeClasspathYaml(collection, "/extensions/opensearch_scalar.yaml");
            extensions = collection;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private static SimpleExtension.ExtensionCollection mergeClasspathYaml(
        SimpleExtension.ExtensionCollection collection,
        String resource
    ) {
        try (java.io.InputStream in = NameBasedScalarFunctionConverterTests.class.getResourceAsStream(resource)) {
            if (in == null) throw new IllegalStateException("missing classpath resource " + resource);
            SimpleExtension.ExtensionCollection custom = SimpleExtension.load(in);
            return collection.merge(custom);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to load " + resource, e);
        }
    }

    /**
     * YEAR(ts) — with no Java adapter registered — must be converted by
     * NameBasedScalarFunctionConverter into date_part('year', ts) via the
     * YAML alias's prepend_args literal injection.
     */
    public void testYearLowersToDatePartYearViaYamlLiteralInjection() {
        RelDataType tsType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);

        SqlFunction yearOp = new SqlFunction(
            "YEAR",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), true)),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode operand = rexBuilder.makeInputRef(tsType, 0);
        RexCall call = (RexCall) rexBuilder.makeCall(yearOp, List.of(operand));

        NameBasedScalarFunctionConverter converter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(),
            typeFactory,
            TypeConverter.DEFAULT
        );

        // The top-level converter for the operand: yield a Substrait field reference.
        java.util.function.Function<RexNode, Expression> topLevel = rex ->
            FieldReference.newRootStructReference(0, N.precisionTimestamp(3));

        Optional<Expression> converted = converter.convert(call, topLevel);

        assertTrue("YEAR must convert via YAML alias — no Java adapter registered", converted.isPresent());
        assertTrue(
            "YEAR must produce a ScalarFunctionInvocation, got " + converted.get().getClass(),
            converted.get() instanceof Expression.ScalarFunctionInvocation
        );
        Expression.ScalarFunctionInvocation inv = (Expression.ScalarFunctionInvocation) converted.get();
        assertEquals("aliased fn name must be date_part", "date_part", inv.declaration().name());

        List<FunctionArg> args = inv.arguments();
        assertEquals("date_part must have 2 args (part, value) after literal injection", 2, args.size());
        assertTrue(
            "arg 0 must be the injected string literal 'year', got " + args.get(0).getClass(),
            args.get(0) instanceof Expression.StrLiteral
        );
        assertEquals("year", ((Expression.StrLiteral) args.get(0)).value());
        assertTrue("arg 1 must be the original operand expression", args.get(1) instanceof Expression);

        // Print the emitted Substrait invocation for the report — makes the
        // "EXPLAIN shape" visible without standing up a cluster.
        logger.info(
            "YEAR(ts) converted to: {}({}, {})",
            inv.declaration().name(),
            ((Expression.StrLiteral) args.get(0)).value(),
            args.get(1)
        );
    }

    /**
     * Regression guard: a name-only string alias (existing shape, e.g. sign → signum)
     * still loads and resolves — the loader's object-form extension must not break
     * the 44 existing string-form entries.
     */
    public void testStringFormAliasStillWorks_SignToSignum() {
        assertEquals("signum", NameBasedScalarFunctionConverter.aliasFor("SIGN"));
        assertEquals("logb", NameBasedScalarFunctionConverter.aliasFor("log"));
        assertEquals("modulus", NameBasedScalarFunctionConverter.aliasFor("mod"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // Stream 1 / Batch 1A — date-part family via YAML prepend_args literal injection.
    // One test per PPL operator name. Each asserts (a) conversion succeeds, (b) the
    // emitted Substrait invocation is date_part with 2 args (injected unit + operand).
    // ────────────────────────────────────────────────────────────────────────

    private Expression.ScalarFunctionInvocation assertDatePartInjection(String opName, String expectedUnit) {
        return assertUnaryTsInjection(opName, "date_part", expectedUnit, /*unitArgIndex=*/0, /*operandArgIndex=*/1);
    }

    /**
     * Synthesize {@code OP(ts)}, run it through {@link NameBasedScalarFunctionConverter},
     * assert it lowers to {@code targetFn(<unit-literal>, ts)} (or {@code targetFn(ts, <literal>)}
     * for append-style injections). Returns the resulting invocation so per-function tests can
     * add extra assertions.
     */
    private Expression.ScalarFunctionInvocation assertUnaryTsInjection(
        String opName,
        String targetFn,
        String expectedLiteral,
        int litArgIndex,
        int operandArgIndex
    ) {
        RelDataType tsType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
        SqlFunction op = new SqlFunction(
            opName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), true)),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.TIMEDATE
        );
        RexNode operand = rexBuilder.makeInputRef(tsType, 0);
        RexCall call = (RexCall) rexBuilder.makeCall(op, List.of(operand));

        NameBasedScalarFunctionConverter converter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT
        );
        java.util.function.Function<RexNode, Expression> topLevel = rex ->
            FieldReference.newRootStructReference(0, N.precisionTimestamp(3));
        Optional<Expression> converted = converter.convert(call, topLevel);

        assertTrue(opName + " must convert via YAML alias — no Java adapter registered", converted.isPresent());
        assertTrue(
            opName + " must produce a ScalarFunctionInvocation, got " + converted.get().getClass(),
            converted.get() instanceof Expression.ScalarFunctionInvocation
        );
        Expression.ScalarFunctionInvocation inv = (Expression.ScalarFunctionInvocation) converted.get();
        assertEquals("aliased fn name must be " + targetFn, targetFn, inv.declaration().name());
        List<FunctionArg> args = inv.arguments();
        assertEquals(targetFn + " must have 2 args after literal injection", 2, args.size());
        assertTrue(
            "lit arg " + litArgIndex + " must be StrLiteral, got " + args.get(litArgIndex).getClass(),
            args.get(litArgIndex) instanceof Expression.StrLiteral
        );
        assertEquals(expectedLiteral, ((Expression.StrLiteral) args.get(litArgIndex)).value());
        assertTrue(
            "operand arg " + operandArgIndex + " must be an Expression",
            args.get(operandArgIndex) instanceof Expression
        );
        return inv;
    }

    public void testMonthLowersToDatePart()          { assertDatePartInjection("MONTH", "month"); }
    public void testMonthOfYearLowersToDatePart()    { assertDatePartInjection("MONTH_OF_YEAR", "month"); }
    public void testDayLowersToDatePart()            { assertDatePartInjection("DAY", "day"); }
    public void testDayOfMonthLowersToDatePart()     { assertDatePartInjection("DAYOFMONTH", "day"); }
    public void testDay_Of_MonthLowersToDatePart()   { assertDatePartInjection("DAY_OF_MONTH", "day"); }
    public void testHourLowersToDatePart()           { assertDatePartInjection("HOUR", "hour"); }
    public void testHourOfDayLowersToDatePart()      { assertDatePartInjection("HOUR_OF_DAY", "hour"); }
    public void testMinuteLowersToDatePart()         { assertDatePartInjection("MINUTE", "minute"); }
    public void testMinuteOfHourLowersToDatePart()   { assertDatePartInjection("MINUTE_OF_HOUR", "minute"); }
    public void testSecondLowersToDatePart()         { assertDatePartInjection("SECOND", "second"); }
    public void testSecondOfMinuteLowersToDatePart() { assertDatePartInjection("SECOND_OF_MINUTE", "second"); }
    public void testDayOfWeekLowersToDatePart()      { assertDatePartInjection("DAYOFWEEK", "dow"); }
    public void testDay_Of_WeekLowersToDatePart()    { assertDatePartInjection("DAY_OF_WEEK", "dow"); }
    public void testDayOfYearLowersToDatePart()      { assertDatePartInjection("DAYOFYEAR", "doy"); }
    public void testDay_Of_YearLowersToDatePart()    { assertDatePartInjection("DAY_OF_YEAR", "doy"); }
    public void testWeekLowersToDatePart()           { assertDatePartInjection("WEEK", "week"); }
    public void testWeekOfYearLowersToDatePart()     { assertDatePartInjection("WEEKOFYEAR", "week"); }
    public void testWeek_Of_YearLowersToDatePart()   { assertDatePartInjection("WEEK_OF_YEAR", "week"); }
    public void testQuarterLowersToDatePart()        { assertDatePartInjection("QUARTER", "quarter"); }
    public void testMicrosecondLowersToDatePart()    { assertDatePartInjection("MICROSECOND", "microsecond"); }

    // ────────────────────────────────────────────────────────────────────────
    // Stream 1 / Batch 1B — DAYNAME, MONTHNAME → to_char(ts, <pattern>) via append_args.
    // ────────────────────────────────────────────────────────────────────────
    public void testDaynameLowersToToChar() {
        assertUnaryTsInjection("DAYNAME", "to_char", "Day", /*litArgIndex=*/1, /*operandArgIndex=*/0);
    }
    public void testMonthnameLowersToToChar() {
        assertUnaryTsInjection("MONTHNAME", "to_char", "Month", /*litArgIndex=*/1, /*operandArgIndex=*/0);
    }

    // ────────────────────────────────────────────────────────────────────────
    // Stream 1 / Batch 1C — RMCOMMA, RMUNIT → regexp_replace(s, <pattern>, '', 'g').
    // Three append_args literals: pattern + empty replacement + global flag.
    // Without the 'g' flag, regexp_replace only substitutes the first match.
    // ────────────────────────────────────────────────────────────────────────
    private void assertUnaryStrTo4ArgRegexReplace(String opName, String expectedPattern) {
        RelDataType strType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SqlFunction op = new SqlFunction(
            opName, SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(strType), null, OperandTypes.ANY,
            SqlFunctionCategory.STRING
        );
        RexNode operand = rexBuilder.makeInputRef(strType, 0);
        RexCall call = (RexCall) rexBuilder.makeCall(op, List.of(operand));

        NameBasedScalarFunctionConverter converter = new NameBasedScalarFunctionConverter(
            extensions.scalarFunctions(), List.of(), typeFactory, TypeConverter.DEFAULT
        );
        java.util.function.Function<RexNode, Expression> topLevel = rex ->
            FieldReference.newRootStructReference(0, N.STRING);
        Optional<Expression> converted = converter.convert(call, topLevel);

        assertTrue(opName + " must convert via YAML alias", converted.isPresent());
        assertTrue(converted.get() instanceof Expression.ScalarFunctionInvocation);
        Expression.ScalarFunctionInvocation inv = (Expression.ScalarFunctionInvocation) converted.get();
        assertEquals("regexp_replace", inv.declaration().name());
        List<FunctionArg> args = inv.arguments();
        assertEquals("regexp_replace(str, pattern, '', 'g') has 4 args", 4, args.size());
        assertTrue(args.get(0) instanceof Expression); // operand
        assertTrue(args.get(1) instanceof Expression.StrLiteral);
        assertTrue(args.get(2) instanceof Expression.StrLiteral);
        assertTrue(args.get(3) instanceof Expression.StrLiteral);
        assertEquals(expectedPattern, ((Expression.StrLiteral) args.get(1)).value());
        assertEquals("", ((Expression.StrLiteral) args.get(2)).value());
        assertEquals("g", ((Expression.StrLiteral) args.get(3)).value());
    }

    public void testRmcommaLowersToRegexpReplace() {
        assertUnaryStrTo4ArgRegexReplace("RMCOMMA", ",");
    }
    public void testRmunitLowersToRegexpReplace() {
        assertUnaryStrTo4ArgRegexReplace("RMUNIT", "[A-Za-z]+$");
    }

    // ────────────────────────────────────────────────────────────────────────
    // Stream 1 / Batch 1D — ILIKE punted to Stream 2.
    // Calcite always emits ILIKE(input, pattern, escape) — the sql-plugin hardcodes
    // the `'\\'` escape operand. YAML alias append_args can't drop operands, so a
    // pure-YAML path would emit a 4-arg regexp_like DF doesn't resolve.
    // IlikeAdapter remains the decomposition path; migration requires either
    // operand-drop support in AliasSpec (extension) or a visitor-layer rewrite.
    // ────────────────────────────────────────────────────────────────────────
    public void testIlikeStillResolvedByAdapter_NotYamlAlias() {
        // Sanity: no ilike alias is present in opensearch_scalar.yaml. This guards
        // against accidental reintroduction of an alias that would double-fire against
        // IlikeAdapter and mis-route 3-arg ILIKE.
        assertEquals("ilike", NameBasedScalarFunctionConverter.aliasFor("ILIKE"));
    }

    // ────────────────────────────────────────────────────────────────────────
    // Stream 1 / Batch 1E — ITEM punted to Stream 2.
    // Calcite's $ITEM is overloaded: struct field access AND array/multiset
    // indexing. ItemArrayElementAdapter discriminates at RexCall rewrite time
    // (container SqlTypeName). A pure YAML alias can't — NameBasedScalarFunctionConverter
    // has no access to operand SqlTypeName, and variant-matching on list<any1>
    // vs struct types would mis-route struct-ITEM. Pre-adapter, struct-ITEM must
    // pass through unchanged to DataFusion's native StructField reference path;
    // a YAML alias would break that invariant.
    // ────────────────────────────────────────────────────────────────────────
    public void testItemStillResolvedByAdapter_NotYamlAlias() {
        // Guard against accidental `item: array_element` YAML alias reintroduction —
        // that would break struct field access by force-routing all ITEM calls
        // through array_element.
        assertEquals("item", NameBasedScalarFunctionConverter.aliasFor("ITEM"));
    }
}
