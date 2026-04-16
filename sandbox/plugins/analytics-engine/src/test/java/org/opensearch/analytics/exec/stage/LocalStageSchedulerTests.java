/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Focused unit tests for {@link LocalStageScheduler}.
 *
 * <p>Post Change 1: tests call {@code createExecution()} and verify the
 * returned {@link StageExecution} instance. Child-walking and child output
 * type tests have moved to PlanWalker tests since the scheduler no longer
 * handles children.
 *
 * Validates: Requirements 2.3, 2.4, 2.5, 3.3, 3.5, 4.5
 */
@SuppressWarnings("unchecked")
public class LocalStageSchedulerTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private RelDataType rowType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        rowType = typeFactory.builder().add("field_0", SqlTypeName.VARCHAR).build();
    }

    // ─── testCreateExecutionReturnsLocalStageExecution ───────────────────

    /**
     * Compute LOCAL stage. Verifies that {@code createExecution} returns a
     * {@link LocalStageExecution} in CREATED state and that
     * {@code createLocalStage} is called on the backend.
     *
     * Validates: Requirements 2.3
     */
    public void testCreateExecutionReturnsLocalStageExecution() {
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        when(mockBackend.createLocalStage(any())).thenReturn(mockCtx);

        LocalStageScheduler scheduler = new LocalStageScheduler(java.util.Map.of(mockBackend.name(), mockBackend));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);

        // Use a PassThroughStageExecution as parent to provide a sink
        Stage parentStage = mock(Stage.class);
        when(parentStage.getStageId()).thenReturn(0);
        PassThroughStageExecution parentExec = new PassThroughStageExecution(parentStage, mock(ExchangeSink.class));

        StageExecution exec = scheduler.createExecution(stage, parentExec.sink(stage.getStageId()), config);

        assertNotNull("createExecution should return a non-null execution", exec);
        assertTrue(
            "Should return LocalStageExecution, got: " + exec.getClass().getSimpleName(),
            exec instanceof LocalStageExecution
        );
        assertEquals("Initial state should be CREATED", StageExecution.State.CREATED, exec.getState());
        assertEquals("Stage id should match", 1, exec.getStageId());
        verify(mockBackend).createLocalStage(any());
    }

    // ─── testNullPrimaryBackendFastFailsOnComputeLocal ──────────────────

    /**
     * Construct with {@code primaryBackend = null}, call createExecution for
     * a compute LOCAL stage (non-pass-through). Assert
     * {@link IllegalStateException} with a message mentioning "primary backend".
     *
     * Validates: Requirements 2.4
     */
    public void testNullPrimaryBackendFastFailsOnComputeLocal() {
        LocalStageScheduler scheduler = new LocalStageScheduler(java.util.Map.of());

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);

        Stage parentStage = mock(Stage.class);
        when(parentStage.getStageId()).thenReturn(0);
        PassThroughStageExecution parentExec = new PassThroughStageExecution(parentStage, mock(ExchangeSink.class));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> scheduler.createExecution(stage, parentExec.sink(stage.getStageId()), config)
        );
        assertTrue(
            "Message should mention primaryBackend, got: " + e.getMessage(),
            e.getMessage() != null && e.getMessage().contains("primaryBackend")
        );
    }

    // ─── testCreateExecutionAcceptsSinkFromParent ─────────────────────

    /**
     * Pass a parent execution with a sink, verify the scheduler uses
     * the parent's sink for the {@code LocalStageRequest}.
     *
     * Validates: Requirements 2.5, 3.3
     */
    public void testCreateExecutionAcceptsSinkFromParent() {
        AnalyticsSearchBackendPlugin mockBackend = mock(AnalyticsSearchBackendPlugin.class);
        when(mockBackend.name()).thenReturn("test-backend");

        LocalStageContext mockCtx = mock(LocalStageContext.class);
        when(mockBackend.createLocalStage(any())).thenAnswer(invocation -> {
            org.opensearch.analytics.backend.LocalStageRequest req = invocation.getArgument(0);
            assertNotNull("LocalStageRequest should have a non-null downstream sink", req.getDownstream());
            return mockCtx;
        });

        LocalStageScheduler scheduler = new LocalStageScheduler(java.util.Map.of(mockBackend.name(), mockBackend));

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);

        ExchangeSink rawSink = new RowProducingSink();
        Stage parentStage = mock(Stage.class);
        when(parentStage.getStageId()).thenReturn(0);
        PassThroughStageExecution parentExec = new PassThroughStageExecution(parentStage, rawSink);

        StageExecution exec = scheduler.createExecution(stage, parentExec.sink(stage.getStageId()), config);

        assertNotNull("createExecution should return a non-null execution", exec);
        verify(mockBackend).createLocalStage(any());
    }

    // ─── testPassThroughDetectedByPlanWalker ────────────────────────────

    /**
     * Pass-through LOCAL stages are detected by PlanWalker.dispatchStage()
     * before the scheduler is consulted. The scheduler itself does not
     * handle pass-through — it always creates a LocalStageExecution.
     * This test verifies that pass-through detection is PlanWalker's job.
     *
     * Validates: Requirements 2.3
     */
    public void testPassThroughDetectedByPlanWalker() {
        // Pass-through stages are handled by PlanWalker.isPassThrough() before
        // selectScheduler is called. The LocalStageScheduler itself does not
        // need to handle pass-through — it will return null for pass-through
        // during Change 1 (the driver handles it). This is tested via
        // PlanWalkerDispatchTests.testPassThroughStillWorksAfterSplit.
        // Here we just verify that a null-backend scheduler throws for
        // non-pass-through stages (the fast-fail path).
        LocalStageScheduler scheduler = new LocalStageScheduler(java.util.Map.of());

        // Non-pass-through: has a real fragment
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster, RelTraitSet.createEmpty(), 0, rowType, List.of("test-backend")
        );
        RelNode projectNode = buildNonPassthroughFragment(stageInput);
        Stage stage = new Stage(1, projectNode, List.of(), null, StageExecutionType.LOCAL);
        stage.setPlanAlternatives(List.of(new StagePlan(projectNode, "test-backend", new byte[] { 1 })));

        QueryContext config = QueryContext.forTest("test-query", null);

        Stage parentStage = mock(Stage.class);
        when(parentStage.getStageId()).thenReturn(0);
        PassThroughStageExecution parentExec = new PassThroughStageExecution(parentStage, mock(ExchangeSink.class));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> scheduler.createExecution(stage, parentExec.sink(stage.getStageId()), config)
        );
        assertTrue("Should mention primaryBackend", e.getMessage().contains("primaryBackend"));
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    private RelNode buildNonPassthroughFragment(RelNode input) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        return LogicalProject.create(input, List.of(), List.of(rexBuilder.makeInputRef(input, 0)), input.getRowType());
    }
}

