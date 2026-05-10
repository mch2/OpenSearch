/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.analytics.spi.WindowFunctionCapability;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for project rule: scalar function validation, opaque operation
 * handling, and painless script delegation.
 */
public class ProjectRuleTests extends BasePlannerRulesTests {

    private static final SqlFunction PAINLESS = new SqlFunction(
        "painless",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction HIGHLIGHT = new SqlFunction(
        "highlight",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    // ---- Simple projection ----

    public void testSimpleFieldProjection() {
        OpenSearchProject result = runProject(
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        for (RexNode expr : result.getProjects()) {
            assertFalse("Field ref should not be annotated", expr instanceof AnnotatedProjectExpression);
        }
    }

    public void testPassthroughProjectionSucceedsWithoutProjectCapability() {
        // A backend that declares NO ProjectCapability should still execute a passthrough
        // projection (only field refs). Verifies the short-circuit in OpenSearchProjectRule.onMatch
        // that skips the backend-refinement gate when no RexCall needs evaluation.
        OpenSearchProject result = runProject(
            MockDataFusionBackend.PARQUET_DATA_FORMAT,
            List.of(new MockDataFusionBackend(), LUCENE),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        for (RexNode expr : result.getProjects()) {
            assertFalse("Passthrough expressions must not be annotated", expr instanceof AnnotatedProjectExpression);
        }
    }

    /**
     * stripAnnotations must re-stamp RexInputRef types from the new input. If the stripped
     * child's column types differ (e.g. an upstream aggregate re-derived COUNT to BIGINT),
     * stale RexInputRef types fail LogicalProject's isValid check and kill the JVM.
     */
    public void testStripAnnotationsRestampsInputRefTypes() {
        RelOptTable origTable = mockTable(
            "test_index",
            new String[] { "k", "v" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        RelNode origScan = stubScan(origTable);
        RexNode refV = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
        OpenSearchProject project = new OpenSearchProject(
            origScan.getCluster(),
            origScan.getTraitSet(),
            origScan,
            ImmutableList.of(refV),
            typeFactory.builder().add("v", SqlTypeName.INTEGER).build(),
            List.of(MockDataFusionBackend.NAME)
        );

        RelOptTable newTable = mockTable(
            "test_index",
            new String[] { "k", "v" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.BIGINT }
        );
        RelNode newScan = stubScan(newTable);

        RelNode result = project.stripAnnotations(List.of(newScan));

        LogicalProject logicalProject = (LogicalProject) result;
        SqlTypeName actual = logicalProject.getProjects().getFirst().getType().getSqlTypeName();
        assertEquals("Project's RexInputRef type must follow the new input column type", SqlTypeName.BIGINT, actual);
    }

    public void testExpressionProjectionStillRequiresCapabilityWithoutDeclaration() {
        // Negative guard: the short-circuit must apply only to passthrough. If a RexCall is
        // present and the backend declares no matching scalar ProjectCapability, the rule must
        // still throw — otherwise a later refactor could silently loosen the gate too much.
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(castExpr), List.of("casted"));
        PlannerContext context = buildContext("parquet", nameValueFields(), List.of(new MockDataFusionBackend(), LUCENE));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    // ---- Scalar functions ----

    public void testSupportedScalarFunction() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        OpenSearchProject result = runProject(castExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testUnsupportedScalarFunctionErrors() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(castExpr), List.of("casted"));
        PlannerContext context = buildContext("parquet", nameValueFields());

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    /**
     * PPL emits {@code SCALAR_MAX(a, b, c)} as a UDF whose return type is {@link SqlTypeName#ANY}
     * — a consequence of the underlying {@code ScalarMaxFunction} being polymorphic across numeric
     * and string types. The project rule must not reject such calls outright; instead it should
     * fall back to inferring the operand type (DOUBLE here) so downstream backend capability
     * dispatch proceeds normally. The actual operator rewrite to {@code GREATEST} happens later
     * via the backend's {@code ScalarFunctionAdapter}.
     */
    public void testScalarFunctionWithAnyReturnTypeUsesOperandFallback() {
        SqlFunction scalarMaxUdf = new SqlFunction(
            "SCALAR_MAX",
            SqlKind.OTHER_FUNCTION,
            opBinding -> typeFactory.createSqlType(SqlTypeName.ANY),
            null,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        // Reference the INTEGER column (index 1) from the stub scan's (VARCHAR, INTEGER) schema.
        // The operand-type fallback must resolve INTEGER → FieldType.INTEGER so the backend
        // capability lookup succeeds.
        RexNode intRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
        RexNode expr = rexBuilder.makeCall(scalarMaxUdf, intRef, intRef);
        assertSame("precondition: UDF return type must be ANY", SqlTypeName.ANY, expr.getType().getSqlTypeName());

        OpenSearchProject result = runProject(expr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Delegation ----

    public void testPainlessDelegationFromDataFusionToLucene() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless"),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    public void testPainlessErrorsWithoutDelegation() {
        // Lucene supports painless but no delegation configured
        MockLuceneBackend luceneWithPainless = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };
        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted_field"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(DATAFUSION, luceneWithPainless)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testMixedFieldAndPainlessWithDelegation() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless"),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
    }

    public void testHighlightDelegation() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless", "highlight"),
            rexBuilder.makeCall(HIGHLIGHT, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    // ---- Opaque natively supported ----

    public void testOpaqueOperationSupportedNatively() {
        MockDataFusionBackend dfWithPainless = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return combine(
                    scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class)),
                    opaqueCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), "painless")
                );
            }
        };
        OpenSearchProject result = runProject(
            "parquet",
            List.of(dfWithPainless, LUCENE),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Nested expressions ----

    public void testNestedScalarFunctions() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
        );
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            castExpr,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        OpenSearchProject result = runProject(plusExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testStripAnnotationsRecursivelyUnwrapsNestedExpressions() {
        // PLUS(CEIL(value), value) — a scalar call with another scalar call as an operand.
        // The project rule recurses into operands (annotateExpr lines 127-139), so both PLUS
        // and the inner CEIL get wrapped in AnnotatedProjectExpression. stripAnnotations must
        // remove every wrapper at every depth before the plan reaches the backend
        // FragmentConvertor — Substrait isthmus has no converter for ANNOTATED_PROJECT_EXPR and
        // would throw "Unable to convert call". (COALESCE would be the natural shape here since
        // PPL fillnull lowers to it, but Calcite's makeCall simplifies COALESCE on non-nullable
        // operands away into the first arg, defeating the test. PLUS+CEIL preserves the
        // nested-call structure we want to exercise.)
        RexNode value = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
        RexNode ceilCall = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, value);
        RexNode plusCall = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, ceilCall, value);
        OpenSearchProject annotated = runProject(plusCall);

        // Sanity: confirm the rule produced the nested-wrapper shape this test exercises.
        RexNode topLevel = annotated.getProjects().get(0);
        assertTrue("Outer PLUS must be annotated", topLevel instanceof AnnotatedProjectExpression);
        RexCall outerOriginal = (RexCall) ((AnnotatedProjectExpression) topLevel).getOriginal();
        assertTrue(
            "Inner CEIL must also be annotated (recursive annotateExpr behavior)",
            outerOriginal.getOperands().get(0) instanceof AnnotatedProjectExpression
        );

        // Strip and assert no AnnotatedProjectExpression survives anywhere in the RexNode tree.
        RelNode stripped = annotated.stripAnnotations(annotated.getInputs());
        assertTrue("Stripped plan should be a plain LogicalProject", stripped instanceof LogicalProject);
        for (RexNode expr : ((LogicalProject) stripped).getProjects()) {
            assertNoAnnotationInTree(expr);
        }
    }

    private static void assertNoAnnotationInTree(RexNode node) {
        assertFalse(
            "Expression tree must not contain AnnotatedProjectExpression after strip: " + node,
            node instanceof AnnotatedProjectExpression
        );
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                assertNoAnnotationInTree(operand);
            }
        }
    }

    // ---- Mixed backends in one projection ----

    public void testMixedBackendsInProjection() {
        MockDataFusionBackend dfWithScalarsAndDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };

        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );

        OpenSearchProject result = runProject(
            "parquet",
            List.of(dfWithScalarsAndDelegation, luceneAccepting),
            fieldRef,
            painlessExpr,
            castExpr
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
        assertAnnotation(result.getProjects().get(2), MockDataFusionBackend.NAME);
    }

    public void testScalarWrappingOpaqueOp() {
        MockDataFusionBackend dfWithScalarsAndDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };

        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), painlessExpr),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );

        OpenSearchProject result = runProject("parquet", List.of(dfWithScalarsAndDelegation, luceneAccepting), plusExpr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
        AnnotatedProjectExpression outerAnnotation = (AnnotatedProjectExpression) result.getProjects().get(0);
        RexNode innerPlus = outerAnnotation.getOriginal();
        assertTrue(innerPlus instanceof RexCall);
        RexNode castOperand = ((RexCall) innerPlus).getOperands().get(0);
        assertAnnotation(castOperand, MockDataFusionBackend.NAME);
        RexNode painlessInside = ((AnnotatedProjectExpression) castOperand).getOriginal();
        assertTrue(painlessInside instanceof RexCall);
        RexNode painlessArg = ((RexCall) painlessInside).getOperands().get(0);
        assertAnnotation(painlessArg, MockLuceneBackend.NAME);
    }

    // ---- Delegation edge cases ----

    public void testDelegationFailsWhenAcceptorLacksOpaqueOp() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless", "highlight");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        // Third backend declares "suggest" so isOpaqueOperation returns true, but no acceptor handles it
        MockLuceneBackend thirdBackend = new MockLuceneBackend() {
            @Override
            public String name() {
                return "mock-third";
            }

            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "suggest");
            }
        };

        SqlFunction suggest = new SqlFunction(
            "suggest",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode suggestExpr = rexBuilder.makeCall(suggest, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(suggestExpr), List.of("sg"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(dfWithDelegation, luceneAccepting, thirdBackend)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testDelegationFailsWhenAcceptorRejectsDelegationType() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        // Lucene supports painless but doesn't accept PROJECT delegation
        MockLuceneBackend luceneWithPainlessNoAccept = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(dfWithDelegation, luceneWithPainlessNoAccept)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    // ---- Composed pipeline shapes ----

    /**
     * Project(Filter(Scan)) — verifies annotation propagation through filter→project
     * at every level.
     */
    public void testProjectOnFilteredScan() {
        RelNode filter = makeFilter(
            stubScan(
                mockTable("test_index", new String[] { "name", "value" }, new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER })
            ),
            makeEquals(1, SqlTypeName.INTEGER, 100)
        );
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        List<String> fieldNames = List.of("col_0");
        LogicalProject project = LogicalProject.create(filter, List.of(), List.of(castExpr), fieldNames);
        PlannerContext context = buildContext("parquet", nameValueFields(), List.of(dfWithScalarFunctions(), LUCENE));
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchFilter.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    /**
     * Project(Agg(Scan)) — single shard: Project → Aggregate(FINAL) → ExchangeReducer →
     * Aggregate(PARTIAL) → Scan, same shape as multi-shard now that scans always declare
     * RANDOM distribution.
     */
    public void testProjectOnAggregateScanSingleShard() {
        RelNode result = runProjectOnAgg(1);
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchAggregate.class, OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testProjectOnAggregateScanMultiShard() {
        RelNode result = runProjectOnAgg(2);
        assertPipelineViableBackends(
            result,
            List.of(
                OpenSearchProject.class,
                OpenSearchAggregate.class,
                OpenSearchExchangeReducer.class,
                OpenSearchAggregate.class,
                OpenSearchTableScan.class
            ),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    private RelNode runProjectOnAgg(int shardCount) {
        RelNode agg = makeAggregate(
            stubScan(
                mockTable("test_index", new String[] { "name", "value" }, new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER })
            ),
            sumCall()
        );
        // Cast SUM result (field 1, INTEGER→VARCHAR) — genuine RexCall that gets annotated
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(agg.getRowType().getFieldList().get(1).getType(), 1)
        );
        LogicalProject project = LogicalProject.create(agg, List.of(), List.of(castExpr), List.of("col_0"));
        PlannerContext context = buildContext("parquet", shardCount, nameValueFields(), List.of(dfWithScalarFunctions(), LUCENE));
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan ({} shard(s)):\n{}", shardCount, RelOptUtil.toString(result));
        return result;
    }

    // ---- Helpers ----

    private static Map<String, Map<String, Object>> nameValueFields() {
        return Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "integer"));
    }

    private void assertAnnotation(RexNode expr, String expectedBackend) {
        assertTrue(
            "Expected AnnotatedProjectExpression, got " + expr.getClass().getSimpleName(),
            expr instanceof AnnotatedProjectExpression
        );
        assertTrue(((AnnotatedProjectExpression) expr).getViableBackends().contains(expectedBackend));
    }

    private OpenSearchProject runProject(RexNode... exprs) {
        return runProject("parquet", List.of(dfWithScalarFunctions(), LUCENE), Set.of(MockDataFusionBackend.NAME), exprs);
    }

    private OpenSearchProject runProject(String format, List<AnalyticsSearchBackendPlugin> backends, RexNode... exprs) {
        return runProject(format, backends, Set.of(MockDataFusionBackend.NAME), exprs);
    }

    private OpenSearchProject runProject(
        String format,
        List<AnalyticsSearchBackendPlugin> backends,
        Set<String> expectedViable,
        RexNode... exprs
    ) {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < exprs.length; i++)
            fieldNames.add("col_" + i);
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(exprs), fieldNames);
        PlannerContext context = buildContext(format, nameValueFields(), backends);
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchProject", result instanceof OpenSearchProject);
        assertPipelineViableBackends(result, List.of(OpenSearchProject.class, OpenSearchTableScan.class), expectedViable);
        return (OpenSearchProject) result;
    }

    /** DF backend with all scalar functions declared. */
    private MockDataFusionBackend dfWithScalarFunctions() {
        return new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
        };
    }

    /** DF (with PROJECT delegation) + Lucene (accepting, with given opaque ops). */
    private List<AnalyticsSearchBackendPlugin> delegationBackends(String... opaqueOps) {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), opaqueOps);
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        return List.of(df, lucene);
    }

    @SafeVarargs
    private static Set<ProjectCapability> combine(Set<ProjectCapability>... sets) {
        Set<ProjectCapability> result = new HashSet<>();
        for (Set<ProjectCapability> set : sets)
            result.addAll(set);
        return result;
    }

    private static Set<ProjectCapability> scalarCaps(Set<String> formats, Set<ScalarFunction> functions) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (ScalarFunction func : functions) {
            caps.add(new ProjectCapability.Scalar(func, Set.of(FieldType.values()), formats, true));
        }
        return caps;
    }

    private static Set<ProjectCapability> opaqueCaps(Set<String> formats, String... names) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (String name : names)
            caps.add(new ProjectCapability.Opaque(name, formats));
        return caps;
    }

    // ---- Window functions (RexOver in a Project) ----

    /** Running-sum shape: {@code SUM(value) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)}. */
    private RexNode makeRunningSumOver() {
        return rexBuilder.makeOver(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)),
            List.of(),
            ImmutableList.<RexFieldCollation>of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,  // rows (vs range)
            true,  // allowPartial
            false, // nullWhenCountZero
            false, // distinct
            false  // ignoreNulls
        );
    }

    public void testWindowFunctionNativelySupported() {
        MockDataFusionBackend dfWithSumWindow = new MockDataFusionBackend() {
            @Override
            protected Set<WindowFunctionCapability> windowFunctionCapabilities() {
                return Set.of(
                    new WindowFunctionCapability(
                        WindowFunction.SUM,
                        Set.of(FieldType.INTEGER),
                        Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT)
                    )
                );
            }
        };
        OpenSearchProject result = runWindowedProject("parquet", List.of(dfWithSumWindow, LUCENE), makeRunningSumOver());
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testWindowFunctionUnsupportedErrors() {
        // Backend declares no windowFunctionCapabilities → the rule must reject a RexOver.
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(makeRunningSumOver()), List.of("running_sum"));
        PlannerContext context = buildContext("parquet", nameValueFields());

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(
            "Expected message about no backend supporting window function, got: " + exception.getMessage(),
            exception.getMessage().contains("window function")
        );
    }

    /**
     * Variant of {@link #runProject(String, List, RexNode...)} for projections whose first
     * expression is a {@link org.apache.calcite.rex.RexOver}. Asserts the post-CBO pipeline is
     * {@code [OpenSearchProject, OpenSearchExchangeReducer, OpenSearchTableScan]} — i.e. that
     * {@link OpenSearchProject#computeSelfCost} forced its input to SINGLETON, causing Volcano
     * to insert a reducer below the windowed Project so the windowed compute runs after gather.
     * Without that requirement the running window would execute per-shard and produce
     * incorrect global results on a multi-shard index.
     */
    private OpenSearchProject runWindowedProject(String format, List<AnalyticsSearchBackendPlugin> backends, RexNode... exprs) {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < exprs.length; i++)
            fieldNames.add("col_" + i);
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(exprs), fieldNames);
        PlannerContext context = buildContext(format, nameValueFields(), backends);
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchProject", result instanceof OpenSearchProject);
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchExchangeReducer.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        return (OpenSearchProject) result;
    }

    public void testWindowFunctionStripReturnsOriginalRexOver() {
        // strip path must unwrap AnnotatedProjectExpression back to the original RexOver so
        // isthmus's RexExpressionConverter can dispatch to visitOver(RexOver).
        MockDataFusionBackend dfWithSumWindow = new MockDataFusionBackend() {
            @Override
            protected Set<WindowFunctionCapability> windowFunctionCapabilities() {
                return Set.of(
                    new WindowFunctionCapability(
                        WindowFunction.SUM,
                        Set.of(FieldType.INTEGER),
                        Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT)
                    )
                );
            }
        };
        OpenSearchProject annotated = runWindowedProject("parquet", List.of(dfWithSumWindow, LUCENE), makeRunningSumOver());
        RelNode stripped = annotated.stripAnnotations(annotated.getInputs());
        assertTrue("Stripped plan should be a plain LogicalProject", stripped instanceof LogicalProject);
        RexNode strippedExpr = ((LogicalProject) stripped).getProjects().get(0);
        assertTrue(
            "Stripped RexNode must be the original RexOver, got " + strippedExpr.getClass().getSimpleName(),
            strippedExpr instanceof org.apache.calcite.rex.RexOver
        );
    }
}
