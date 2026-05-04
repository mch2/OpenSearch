/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;

/**
 * Verifies that the percentile fraction literal survives the substrait plan roundtrip
 * as a {@code literal} argument, not as a {@code selection} field reference.
 *
 * <p>Background: Calcite's {@code RelBuilder.AggCall.Registrar} wraps literal
 * aggregate arguments into a synthesized pre-{@code Project} column and references
 * that column from the aggregate's arg list. Isthmus then emits the aggregate's
 * fraction arg as a {@code FieldReference} into that Project — which DataFusion's
 * substrait consumer rejects with {@code "Percentile value for 'APPROX_PERCENTILE_CONT'
 * must be a literal"}. {@link SubstraitPlanRewriter} splices the literal back into
 * the aggregate measure post-isthmus so DataFusion sees a literal arg.
 */
public class PercentileFractionSpliceTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    /**
     * A bare-bones stand-in for the PPL {@code percentile_approx} aggregate whose name matches
     * the lookup key in {@link io.substrait.isthmus.expression.NameBasedAggregateFunctionConverter}'s
     * name-alias table ({@code percentile_approx → approx_percentile_cont}). That converter
     * falls back to name+arity matching when the strict directMap lookup misses, so this
     * stock SqlBasicAggFunction is enough to drive a realistic isthmus conversion without
     * requiring a dependency on the sql-plugin repo's PPLBuiltinOperators.
     */
    private static final SqlAggFunction PERCENTILE_APPROX = SqlBasicAggFunction.create(
        "percentile_approx",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_FORCE_NULLABLE,
        OperandTypes.NUMERIC_NUMERIC
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        // Mirror DataFusionPlugin#loadSubstraitExtensions' TCCL swap + custom YAML
        // merge so the NameBasedAggregateFunctionConverter can resolve
        // `percentile_approx` to `approx_percentile_cont` (declared in
        // opensearch_aggregate.yaml — not present in the stock substrait catalog).
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(PercentileFractionSpliceTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            try (java.io.InputStream stream =
                PercentileFractionSpliceTests.class.getResourceAsStream("/extensions/opensearch_aggregate.yaml")) {
                assertNotNull("opensearch_aggregate.yaml must be on the test classpath", stream);
                collection = collection.merge(SimpleExtension.load(stream));
            }
            extensions = collection;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /**
     * {@code Aggregate(percentile_approx(balance, 0.5))} over a Calcite plan
     * whose Project column 1 is the fraction literal 0.5. After isthmus emits
     * the aggregate with a FieldReference for the fraction, the splicer in
     * {@link SubstraitPlanRewriter} must replace that FieldReference with the
     * FP64 literal 0.5.
     */
    public void testPercentileFractionLiteralSplicedIntoAggregate() throws Exception {
        // Input row type: [balance: BIGINT] — the Project below appends the fraction literal.
        RelDataTypeFactory.Builder tb = typeFactory.builder();
        tb.add("balance", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true));
        RelDataType scanRowType = tb.build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "accounts", scanRowType);

        // Pre-aggregate Project mirrors the Calcite RelBuilder.AggCall.Registrar shape:
        // [balance_ref, literal_0.5]. The aggregate will reference both by index.
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        LogicalProject pre = LogicalProject.create(
            scan,
            List.of(),
            List.of(
                rexBuilder.makeInputRef(scan, 0),
                rexBuilder.makeLiteral(0.5, doubleType, /* allowCast */ false)
            ),
            List.of("balance", "pct"),
            java.util.Set.of()
        );

        AggregateCall percentileCall = AggregateCall.create(
            PERCENTILE_APPROX,
            false, // distinct
            false, // approximate
            false, // ignoreNulls
            List.of(),
            List.of(0, 1), // [balance, pct] positional refs into the Project
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "p50_balance"
        );
        LogicalAggregate agg = LogicalAggregate.create(
            pre, List.of(), ImmutableBitSet.of(), null, List.of(percentileCall));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertShardScanFragment("accounts", agg);
        assertNotNull(bytes);

        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue("root must be an AggregateRel", root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertFalse("aggregate must have at least one measure", aggRel.getMeasuresList().isEmpty());
        AggregateFunction fn = aggRel.getMeasures(0).getMeasure();
        assertEquals("percentile_approx emits two operands [field, fraction]", 2, fn.getArgumentsCount());

        // The field operand (first) is still a field reference into the Project.
        FunctionArgument arg0 = fn.getArguments(0);
        assertTrue("first arg must be an expression", arg0.hasValue());
        Expression arg0Expr = arg0.getValue();
        assertTrue("first arg must be a FieldReference into Project", arg0Expr.hasSelection());

        // The fraction operand (second) MUST be a literal after the splicer runs —
        // not a field reference. This is the whole point of the splicer.
        FunctionArgument arg1 = fn.getArguments(1);
        assertTrue("second arg must be an expression", arg1.hasValue());
        Expression arg1Expr = arg1.getValue();
        assertFalse(
            "second arg must NOT be a FieldReference — splicer must have inlined the literal;"
                + " actual expr: " + arg1Expr,
            arg1Expr.hasSelection()
        );
        assertTrue("second arg must be a literal; got: " + arg1Expr, arg1Expr.hasLiteral());
        Expression.Literal lit = arg1Expr.getLiteral();
        assertTrue("fraction literal must be FP64 (DOUBLE); got: " + lit, lit.hasFp64());
        assertEquals("fraction literal must equal 0.5", 0.5, lit.getFp64(), 0.0);
    }

    /**
     * Sanity check: a non-percentile aggregate (SUM) must be left alone.
     * The splicer must only touch {@code approx_percentile_cont} measures.
     */
    public void testNonPercentileAggregateUntouched() throws Exception {
        RelDataTypeFactory.Builder tb = typeFactory.builder();
        tb.add("balance", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true));
        RelDataType scanRowType = tb.build();

        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "accounts", scanRowType);

        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true),
            "sum_balance"
        );
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertShardScanFragment("accounts", agg);
        Plan plan = Plan.parseFrom(bytes);
        Rel root = rootRel(plan);
        assertTrue(root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals(1, aggRel.getMeasuresCount());
        AggregateFunction fn = aggRel.getMeasures(0).getMeasure();
        // SUM takes one arg, still a FieldReference.
        assertEquals(1, fn.getArgumentsCount());
        assertTrue("SUM's single arg must remain a FieldReference", fn.getArguments(0).getValue().hasSelection());
    }

    private static Rel rootRel(Plan plan) {
        assertFalse(plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue(planRel.hasRoot());
        return planRel.getRoot().getInput();
    }
}
