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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.be.datafusion.DataFusionFragmentConvertor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.InputStream;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;

/**
 * Tests the {@code ARG_MIN}/{@code ARG_MAX} → {@code first_value}/{@code last_value}
 * rewrite in {@link NameBasedAggregateFunctionConverter}. Feeds a Calcite
 * {@link LogicalAggregate} whose single measure is an {@link SqlStdOperatorTable#ARG_MIN}
 * or {@link SqlStdOperatorTable#ARG_MAX} call through
 * {@link DataFusionFragmentConvertor#convertFinalAggFragment}, decodes the resulting
 * Substrait proto bytes, and asserts:
 *
 * <ul>
 *   <li>the emitted aggregate measure has exactly one argument (the value field), NOT
 *       two — the key field is pulled out into the sort field list;</li>
 *   <li>the measure carries exactly one sort field whose expression is a field
 *       reference to the key and whose direction is {@code ASC_NULLS_LAST};</li>
 *   <li>the function reference points at {@code first_value} for ARG_MIN and
 *       {@code last_value} for ARG_MAX.</li>
 * </ul>
 */
public class NameBasedAggregateFunctionConverterTests extends OpenSearchTestCase {

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

        // Load the substrait core defaults plus OpenSearch's aggregate-extension
        // YAML — the latter carries the first_value / last_value variants our
        // rewrite looks up by name.
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(NameBasedAggregateFunctionConverterTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection collection = DefaultExtensionCatalog.DEFAULT_COLLECTION;
            try (
                InputStream stream = NameBasedAggregateFunctionConverterTests.class.getResourceAsStream(
                    "/extensions/opensearch_aggregate.yaml"
                )
            ) {
                assertNotNull("opensearch_aggregate.yaml must be on the test classpath", stream);
                collection = collection.merge(SimpleExtension.load(stream));
            }
            extensions = collection;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /**
     * {@code SELECT ARG_MIN(value, key) FROM t} lowered via isthmus must emit a
     * substrait measure targeting {@code first_value(value)} with
     * {@code sorts=[{expr=key, direction=ASC_NULLS_LAST}]}.
     */
    public void testArgMinRewritesToFirstValueWithSortKey() throws Exception {
        assertArgMinMaxRewrite(SqlStdOperatorTable.ARG_MIN, "first_value");
    }

    /**
     * Symmetric to {@link #testArgMinRewritesToFirstValueWithSortKey()} — ARG_MAX
     * routes to {@code last_value(value)} with the same ASC_NULLS_LAST sort field.
     * {@code last_value} with ASC returns the row with the max key because it's the
     * last row after sorting.
     */
    public void testArgMaxRewritesToLastValueWithSortKey() throws Exception {
        assertArgMinMaxRewrite(SqlStdOperatorTable.ARG_MAX, "last_value");
    }

    /**
     * Shared assertion body — builds an aggregate fragment with one call using
     * {@code op(value_col, key_col)}, runs it through the fragment convertor,
     * and checks the resulting proto's first measure.
     */
    private void assertArgMinMaxRewrite(org.apache.calcite.sql.SqlAggFunction op, String expectedVariantName) throws Exception {
        RelNode leaf = new OpenSearchStageInputScan(cluster, cluster.traitSet(), 0, rowType("value_col", "key_col"), List.of("datafusion"));
        AggregateCall call = AggregateCall.create(
            op,
            /* isDistinct */ false,
            /* argList */ List.of(0, 1),
            /* filterArg */ -1,
            /* type */ typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            /* name */ "result"
        );
        LogicalAggregate agg = LogicalAggregate.create(leaf, List.of(), ImmutableBitSet.of(), /* groupSets */ null, List.of(call));

        byte[] bytes = new DataFusionFragmentConvertor(extensions).convertFinalAggFragment(agg);
        Plan plan = Plan.parseFrom(bytes);
        assertFalse("plan must have at least one relation", plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue("plan relation must carry a root", planRel.hasRoot());
        Rel root = planRel.getRoot().getInput();
        assertTrue("root must be an AggregateRel for " + op.getName(), root.hasAggregate());
        AggregateRel aggRel = root.getAggregate();
        assertEquals("expected exactly one measure", 1, aggRel.getMeasuresCount());
        io.substrait.proto.AggregateFunction fn = aggRel.getMeasures(0).getMeasure();

        // Measure must have exactly 1 argument — the value field. The key field was
        // pulled out into the sort field list by the rewrite.
        assertEquals("rewritten measure must carry 1 argument (the value field), not 2", 1, fn.getArgumentsCount());

        // Measure must carry exactly one sort field, expr = field-reference to the key,
        // direction = ASC_NULLS_LAST.
        assertEquals("rewritten measure must carry exactly one sort field", 1, fn.getSortsCount());
        SortField sort = fn.getSorts(0);
        assertEquals(
            "rewrite emits ASC_NULLS_LAST direction for both ARG_MIN and ARG_MAX",
            SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST,
            sort.getDirection()
        );
        assertTrue("sort expression must be a field reference to the key column", sort.getExpr().hasSelection());
        assertEquals(
            "sort expression must reference column index 1 (key_col)",
            1,
            sort.getExpr().getSelection().getDirectReference().getStructField().getField()
        );

        // Function reference in the plan's extensions must point at first_value or last_value.
        int fnRef = fn.getFunctionReference();
        String resolvedName = plan.getExtensionsList()
            .stream()
            .filter(ext -> ext.hasExtensionFunction() && ext.getExtensionFunction().getFunctionAnchor() == fnRef)
            .map(ext -> ext.getExtensionFunction().getName())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no extension function entry for anchor " + fnRef));
        assertTrue(
            "rewritten function name must start with " + expectedVariantName + " (got: " + resolvedName + ")",
            resolvedName.startsWith(expectedVariantName)
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private RelDataType rowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    // Keep the sort-field type reachable so Eclipse's organize-imports doesn't prune it.
    @SuppressWarnings("unused")
    private static final Class<?> SORT_FIELD_CLASS = Expression.SortField.class;

    @SuppressWarnings("unused")
    private static final Class<?> INVOCATION_CLASS = AggregateFunctionInvocation.class;
}
