/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;

public class SubstraitPlanRewriterTests extends OpenSearchTestCase {

    private static final TypeCreator R = TypeCreator.of(false);

    public void testTimestampPrecision6ConvertedTo3() {
        long epochMicros = 1704067200000000L; // 2024-01-01T00:00:00Z in micros
        long expectedMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMicros)
            .precision(6)
            .nullable(false)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testTimestampPrecision9ConvertedTo3() {
        long epochNanos = 1704067200000000000L; // 2024-01-01T00:00:00Z in nanos
        long expectedMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder().value(epochNanos).precision(9).nullable(false).build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testTimestampPrecision3Unchanged() {
        long epochMillis = 1704067200000L;

        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMillis)
            .precision(3)
            .nullable(false)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) condition;
        assertEquals(3, pts.precision());
        assertEquals(epochMillis, pts.value());
    }

    public void testTimestampInsideScalarFunction() {
        long epochMicros = 1704067200000000L;
        long expectedMillis = 1704067200000L;

        Expression tsLiteral = ImmutableExpression.PrecisionTimestampLiteral.builder()
            .value(epochMicros)
            .precision(6)
            .nullable(false)
            .build();

        FieldReference fieldRef = FieldReference.newRootStructReference(0, R.precisionTimestamp(3));

        SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        SimpleExtension.ScalarFunctionVariant gtFunc = extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "gt:any_any")
        );

        Expression gtCall = Expression.ScalarFunctionInvocation.builder()
            .declaration(gtFunc)
            .addArguments(fieldRef, tsLiteral)
            .outputType(R.BOOLEAN)
            .build();

        Plan plan = buildFilterPlan(gtCall);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.ScalarFunctionInvocation);
        Expression.ScalarFunctionInvocation rewrittenGt = (Expression.ScalarFunctionInvocation) condition;
        Expression arg1 = (Expression) rewrittenGt.arguments().get(1);
        assertTrue(arg1 instanceof Expression.PrecisionTimestampLiteral);
        Expression.PrecisionTimestampLiteral pts = (Expression.PrecisionTimestampLiteral) arg1;
        assertEquals(3, pts.precision());
        assertEquals(expectedMillis, pts.value());
    }

    public void testCatalogPrefixStripped() {
        NamedScan scan = NamedScan.builder()
            .names(List.of("opensearch", "parquet_dates"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.I64)))
            .build();

        Plan plan = buildPlan(scan);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        NamedScan rewrittenScan = (NamedScan) rewritten.getRoots().get(0).getInput();
        assertEquals(List.of("parquet_dates"), rewrittenScan.getNames());
    }

    public void testUnsupportedPrecisionThrows() {
        Expression literal = ImmutableExpression.PrecisionTimestampLiteral.builder().value(12345L).precision(4).nullable(false).build();

        Plan plan = buildFilterPlan(literal);
        expectThrows(IllegalArgumentException.class, () -> SubstraitPlanRewriter.rewrite(plan));
    }

    // --- VarChar literal rewrites ---
    // Calcite's constant folding of JSON_ARRAY/JSON_OBJECT produces a VARCHAR(2000)
    // RexLiteral. substrait-isthmus's LiteralConverter emits these as parameterized
    // VarCharLiteral, which DataFusion's substrait consumer rejects with
    // "Unsupported literal_type: VarChar". Rewrite them to StrLiteral here so the
    // consumer can handle them as plain strings.

    public void testVarCharLiteralRewrittenToStrLiteral() {
        Expression literal = ImmutableExpression.VarCharLiteral.builder()
            .value("[\"1\",\"2\",\"3\"]")
            .length(2000)
            .nullable(false)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue("Expected StrLiteral, got " + condition.getClass(), condition instanceof Expression.StrLiteral);
        Expression.StrLiteral str = (Expression.StrLiteral) condition;
        assertEquals("[\"1\",\"2\",\"3\"]", str.value());
        assertFalse(str.nullable());
    }

    public void testVarCharLiteralNullabilityPreserved() {
        Expression literal = ImmutableExpression.VarCharLiteral.builder()
            .value("hello")
            .length(10)
            .nullable(true)
            .build();

        Plan plan = buildFilterPlan(literal);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.StrLiteral);
        Expression.StrLiteral str = (Expression.StrLiteral) condition;
        assertEquals("hello", str.value());
        assertTrue(str.nullable());
    }

    public void testVarCharInsideScalarFunction() {
        Expression varChar = ImmutableExpression.VarCharLiteral.builder()
            .value("[1,2,3]")
            .length(2000)
            .nullable(false)
            .build();

        FieldReference fieldRef = FieldReference.newRootStructReference(0, R.STRING);

        SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        SimpleExtension.ScalarFunctionVariant eqFunc = extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "equal:any_any")
        );

        Expression eqCall = Expression.ScalarFunctionInvocation.builder()
            .declaration(eqFunc)
            .addArguments(fieldRef, varChar)
            .outputType(R.BOOLEAN)
            .build();

        NamedScan scan = NamedScan.builder()
            .names(List.of("test_table"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.STRING)))
            .build();
        Filter filter = Filter.builder().input(scan).condition(eqCall).build();
        Plan plan = buildPlan(filter);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Expression condition = getFilterCondition(rewritten);
        assertTrue(condition instanceof Expression.ScalarFunctionInvocation);
        Expression arg1 = (Expression) ((Expression.ScalarFunctionInvocation) condition).arguments().get(1);
        assertTrue("Expected StrLiteral inside function call, got " + arg1.getClass(), arg1 instanceof Expression.StrLiteral);
        assertEquals("[1,2,3]", ((Expression.StrLiteral) arg1).value());
    }

    // --- percentile_approx fraction literal splice ---
    //
    // Calcite's RelBuilder.AggCall hoists non-RexInputRef args into the input Project, so by the
    // time isthmus emits substrait the Aggregate's arg[1] is a FieldReference, not a Literal.
    // DataFusion's approx_percentile_cont rejects anything but Expr::Literal for the fraction arg,
    // so the rewriter splices the Project-produced literal back into the Aggregate's arg list.

    /** Given an Aggregate(approx_percentile_cont(col0, $2)) over Project(col0, col1, 0.5), the
     *  rewriter should replace arg[1] with the literal 0.5 inline so DataFusion's percentile
     *  validator sees an Expr::Literal. */
    public void testPercentileApproxFractionLiteralSpliced() {
        SimpleExtension.AggregateFunctionVariant pctFunc = opensearchAggExtensions().getAggregateFunction(
            SimpleExtension.FunctionAnchor.of("extension:org.opensearch:opensearch_aggregate", "approx_percentile_cont:any_fp64")
        );

        // Scan: (balance:fp64, gender:str)
        NamedScan scan = NamedScan.builder()
            .names(List.of("bank"))
            .initialSchema(NamedStruct.of(List.of("balance", "gender"), R.struct(R.FP64, R.STRING)))
            .build();

        // Project: (balance:fp64, gender:str, 0.5:fp64)
        Expression.FP64Literal halfLit = Expression.FP64Literal.builder().value(0.5).nullable(false).build();
        Project project = Project.builder()
            .input(scan)
            .addExpressions(halfLit)
            .build();

        // Aggregate: approx_percentile_cont(col0, col2) where col0 = balance, col2 = projected 0.5
        FieldReference balanceRef = FieldReference.newRootStructReference(0, R.FP64);
        FieldReference fractionRef = FieldReference.newRootStructReference(2, R.FP64);
        AggregateFunctionInvocation afi = AggregateFunctionInvocation.builder()
            .declaration(pctFunc)
            .addArguments(balanceRef, fractionRef)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .outputType(R.FP64)
            .build();
        Aggregate aggregate = Aggregate.builder()
            .input(project)
            .addMeasures(Aggregate.Measure.builder().function(afi).build())
            .build();

        Plan plan = buildPlan(aggregate);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Aggregate resultAgg = (Aggregate) rewritten.getRoots().get(0).getInput();
        FunctionArg arg1 = resultAgg.getMeasures().get(0).getFunction().arguments().get(1);
        assertTrue(
            "arg[1] must be spliced to a literal, got " + arg1.getClass().getSimpleName(),
            arg1 instanceof Expression.FP64Literal
        );
        assertEquals(0.5, ((Expression.FP64Literal) arg1).value(), 1e-12);
    }

    /** Non-percentile aggregates (e.g. COUNT(*)) with FieldReference args must NOT be touched —
     *  the splice is targeted at the percentile fraction arg only. */
    public void testNonPercentileAggregateUnchanged() {
        // Build SUM(balance) over Project(balance). arg[0] is FieldReference to col 0 —
        // should stay a FieldReference after the rewrite.
        SimpleExtension.ExtensionCollection exts = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        SimpleExtension.AggregateFunctionVariant sumFunc = exts.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:fp64")
        );
        NamedScan scan = NamedScan.builder()
            .names(List.of("bank"))
            .initialSchema(NamedStruct.of(List.of("balance"), R.struct(R.FP64)))
            .build();
        Project project = Project.builder().input(scan).build();
        FieldReference balanceRef = FieldReference.newRootStructReference(0, R.FP64);
        AggregateFunctionInvocation afi = AggregateFunctionInvocation.builder()
            .declaration(sumFunc)
            .addArguments(balanceRef)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .outputType(R.FP64)
            .build();
        Aggregate aggregate = Aggregate.builder()
            .input(project)
            .addMeasures(Aggregate.Measure.builder().function(afi).build())
            .build();

        Plan plan = buildPlan(aggregate);
        Plan rewritten = SubstraitPlanRewriter.rewrite(plan);

        Aggregate resultAgg = (Aggregate) rewritten.getRoots().get(0).getInput();
        FunctionArg arg0 = resultAgg.getMeasures().get(0).getFunction().arguments().get(0);
        assertTrue(
            "SUM's column-ref arg must not be spliced, got " + arg0.getClass().getSimpleName(),
            arg0 instanceof FieldReference
        );
    }

    /** End-to-end shape from the real integration path: isthmus emits Project with a
     *  {@link io.substrait.relation.Rel.Remap} that exposes ONLY the project expressions,
     *  so Aggregate's FieldReference(K) resolves directly to projectExprs[K]. This is the
     *  case that matters for testPercentile / testMedian in production. */
    public void testPercentileApproxFractionLiteralSplicedWithRemap() {
        SimpleExtension.AggregateFunctionVariant pctFunc = opensearchAggExtensions().getAggregateFunction(
            SimpleExtension.FunctionAnchor.of("extension:org.opensearch:opensearch_aggregate", "approx_percentile_cont:any_fp64")
        );

        // Input schema has 13 cols (mirrors the bank index with 13 top-level mapped fields).
        List<String> inputCols = List.of(
            "account_number", "address", "age", "balance", "city", "email",
            "employer", "firstname", "gender", "lastname", "male", "state", "_id"
        );
        io.substrait.type.Type[] inputTypes = {
            R.I64, R.STRING, R.I32, R.FP64, R.STRING, R.STRING,
            R.STRING, R.STRING, R.STRING, R.STRING, R.BOOLEAN, R.STRING, R.STRING
        };
        NamedScan scan = NamedScan.builder()
            .names(List.of("bank"))
            .initialSchema(NamedStruct.of(inputCols, R.struct(inputTypes)))
            .build();

        // Project with remap: outward schema is [balance, 0.5, 0.9] only.
        // In internal space, projectExprs sit at indices [13, 14, 15] but external
        // FieldReferences (from the Aggregate above) use indices [0, 1, 2].
        FieldReference balancePassthrough = FieldReference.newRootStructReference(3, R.FP64);
        Expression.FP64Literal halfLit = Expression.FP64Literal.builder().value(0.5).nullable(false).build();
        Expression.FP64Literal nineNineLit = Expression.FP64Literal.builder().value(0.9).nullable(false).build();
        Project project = Project.builder()
            .input(scan)
            .addExpressions(balancePassthrough, halfLit, nineNineLit)
            .remap(io.substrait.relation.Rel.Remap.offset(inputCols.size(), 3))
            .build();

        // Aggregate: approx_percentile_cont(col 0 = balance, col 1 = 0.5)
        FieldReference balanceRef = FieldReference.newRootStructReference(0, R.FP64);
        FieldReference fractionRef = FieldReference.newRootStructReference(1, R.FP64);
        AggregateFunctionInvocation afi = AggregateFunctionInvocation.builder()
            .declaration(pctFunc)
            .addArguments(balanceRef, fractionRef)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .outputType(R.FP64)
            .build();
        Aggregate aggregate = Aggregate.builder()
            .input(project)
            .addMeasures(Aggregate.Measure.builder().function(afi).build())
            .build();

        Plan rewritten = SubstraitPlanRewriter.rewrite(buildPlan(aggregate));

        Aggregate resultAgg = (Aggregate) rewritten.getRoots().get(0).getInput();
        FunctionArg arg1 = resultAgg.getMeasures().get(0).getFunction().arguments().get(1);
        assertTrue(
            "arg[1] must be spliced to a literal with remap, got " + arg1.getClass().getSimpleName(),
            arg1 instanceof Expression.FP64Literal
        );
        assertEquals(0.5, ((Expression.FP64Literal) arg1).value(), 1e-12);
    }

    /** If the Project expression at the referenced offset is NOT a literal (e.g. it's a cast
     *  over a column), leave the arg alone — the splice only fires for genuine literals. */
    public void testPercentileApproxNonLiteralProjectionLeftAlone() {
        SimpleExtension.AggregateFunctionVariant pctFunc = opensearchAggExtensions().getAggregateFunction(
            SimpleExtension.FunctionAnchor.of("extension:org.opensearch:opensearch_aggregate", "approx_percentile_cont:any_fp64")
        );
        NamedScan scan = NamedScan.builder()
            .names(List.of("bank"))
            .initialSchema(NamedStruct.of(List.of("balance", "pct"), R.struct(R.FP64, R.FP64)))
            .build();
        // Project pulls through balance + pct; computes no literal. arg[1] refs an existing column.
        Project project = Project.builder().input(scan).build();
        FieldReference balanceRef = FieldReference.newRootStructReference(0, R.FP64);
        FieldReference pctRef = FieldReference.newRootStructReference(1, R.FP64);
        AggregateFunctionInvocation afi = AggregateFunctionInvocation.builder()
            .declaration(pctFunc)
            .addArguments(balanceRef, pctRef)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .outputType(R.FP64)
            .build();
        Aggregate aggregate = Aggregate.builder()
            .input(project)
            .addMeasures(Aggregate.Measure.builder().function(afi).build())
            .build();

        Plan rewritten = SubstraitPlanRewriter.rewrite(buildPlan(aggregate));

        Aggregate resultAgg = (Aggregate) rewritten.getRoots().get(0).getInput();
        FunctionArg arg1 = resultAgg.getMeasures().get(0).getFunction().arguments().get(1);
        assertTrue(
            "non-literal projection source must leave arg as FieldReference, got " + arg1.getClass().getSimpleName(),
            arg1 instanceof FieldReference
        );
    }

    // --- helpers ---

    private static Plan buildFilterPlan(Expression condition) {
        NamedScan scan = NamedScan.builder()
            .names(List.of("test_table"))
            .initialSchema(NamedStruct.of(List.of("col0"), R.struct(R.precisionTimestamp(3))))
            .build();

        Filter filter = Filter.builder().input(scan).condition(condition).build();

        return buildPlan(filter);
    }

    private static Plan buildPlan(io.substrait.relation.Rel rel) {
        Plan.Root root = Plan.Root.builder().input(rel).addNames("col0").build();
        return Plan.builder().addRoots(root).build();
    }

    private static Expression getFilterCondition(Plan plan) {
        Filter filter = (Filter) plan.getRoots().get(0).getInput();
        return filter.getCondition();
    }

    /** Loads the opensearch aggregate extension catalog from the plugin resource so tests that
     *  reference {@code approx_percentile_cont} can resolve its AggregateFunctionVariant the
     *  same way production code does via {@link DataFusionPlugin}. */
    private static SimpleExtension.ExtensionCollection opensearchAggExtensions() {
        try (java.io.InputStream stream = SubstraitPlanRewriterTests.class.getResourceAsStream(
            "/extensions/opensearch_aggregate.yaml")) {
            if (stream == null) {
                throw new IllegalStateException("opensearch_aggregate.yaml not on test classpath");
            }
            SimpleExtension.ExtensionCollection layered = SimpleExtension.load(stream);
            return DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(layered);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }
}
