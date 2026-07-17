/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;

import java.util.Collections;

import static org.opensearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;

/**
 * Tests for {@link ResultHeapCharge} — the per-query reservation against the shared REQUEST breaker.
 *
 * <p>Uses a real {@link HierarchyCircuitBreakerService} REQUEST breaker (real-memory tracking off, a
 * fixed byte limit) rather than a mock, so charge / shrinkTo / close are asserted against the same
 * accumulated {@code used} counter aggregations and QueryPhaseResultConsumer charge in production.
 */
public class ResultHeapChargeTests extends org.opensearch.test.OpenSearchTestCase {

    private HierarchyCircuitBreakerService service;

    private CircuitBreaker requestBreaker(long limitBytes) {
        service = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes, ByteSizeUnit.BYTES)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return service.getBreaker(CircuitBreaker.REQUEST);
    }

    /** A factor of 1.0 charges the native bytes verbatim and returns the breaker to zero on close. */
    public void testChargeVerbatimAtUnitFactorAndReleaseOnClose() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q1", 1.0);

        charge.charge(100);
        assertEquals(100, charge.chargedBytes());
        assertEquals(100, breaker.getUsed());

        charge.close();
        assertEquals("close zeroes the running total", 0, charge.chargedBytes());
        assertEquals("close returns the shared breaker to its pre-request level", 0, breaker.getUsed());
    }

    /** The heap-expansion factor scales the native charge symmetrically into the breaker. */
    public void testExpansionFactorScalesTheCharge() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q2", 1.5);

        charge.charge(200); // 200 native × 1.5 = 300 heap
        assertEquals(300, charge.chargedBytes());
        assertEquals(300, breaker.getUsed());

        charge.close();
        assertEquals(0, breaker.getUsed());
    }

    /** shrinkTo hands back the difference between the worst-case reservation and the actual size. */
    public void testShrinkToReleasesTheOverReservation() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q3", 2.0);

        charge.charge(500); // worst-case: 500 × 2.0 = 1000
        assertEquals(1000, breaker.getUsed());

        charge.shrinkTo(100); // actual: 100 × 2.0 = 200
        assertEquals(200, charge.chargedBytes());
        assertEquals(200, breaker.getUsed());

        charge.close();
        assertEquals(0, breaker.getUsed());
    }

    /** shrinkTo never grows a reservation: an actual at/above the charge is a no-op. */
    public void testShrinkToNeverGrows() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q4", 1.0);

        charge.charge(100);
        charge.shrinkTo(1_000); // would grow → ignored
        assertEquals(100, charge.chargedBytes());
        assertEquals(100, breaker.getUsed());

        charge.close();
        assertEquals(0, breaker.getUsed());
    }

    /** shrinkTo(0) releases everything while leaving the reservation open for a later close. */
    public void testShrinkToZeroReleasesAll() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q5", 1.5);

        charge.charge(400);
        charge.shrinkTo(0);
        assertEquals(0, charge.chargedBytes());
        assertEquals(0, breaker.getUsed());

        charge.close(); // still idempotent
        assertEquals(0, breaker.getUsed());
    }

    /** close is idempotent — a second close does not double-release the shared breaker. */
    public void testCloseIsIdempotent() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q6", 1.0);
        charge.charge(100);
        assertEquals(100, breaker.getUsed());
        charge.close();
        charge.close(); // second close must be a safe no-op
        assertEquals(0, charge.chargedBytes());
        assertEquals(0, breaker.getUsed());
    }

    /** Charging after close must not re-open the reservation. */
    public void testChargeAfterCloseIsNoop() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q7", 1.0);
        charge.close();
        charge.charge(100);
        assertEquals(0, charge.chargedBytes());
        assertEquals(0, breaker.getUsed());
    }

    public void testNonPositiveChargeIsNoop() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q8", 1.0);
        charge.charge(0);
        charge.charge(-5);
        assertEquals(0, charge.chargedBytes());
        assertEquals(0, breaker.getUsed());
    }

    /** A charge that would breach the limit throws (→ 429) and leaves the running total unchanged. */
    public void testChargeThrowsWhenBreakerTripsAndTotalUnchanged() {
        CircuitBreaker breaker = requestBreaker(100); // tiny limit
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q9", 1.0);

        expectThrows(CircuitBreakingException.class, () -> charge.charge(200));
        // The breaker rolled back; nothing was added to the running total or the shared counter.
        assertEquals(0, charge.chargedBytes());
        assertEquals(0, breaker.getUsed());

        // Recovery: a charge that fits succeeds.
        charge.charge(42);
        assertEquals(42, charge.chargedBytes());
        assertEquals(42, breaker.getUsed());
        charge.close();
        assertEquals(0, breaker.getUsed());
    }

    /** Two concurrent queries share ONE breaker: their charges sum, and the second trips at the limit. */
    public void testSharedBudgetAcrossTwoQueries() {
        CircuitBreaker breaker = requestBreaker(1_000);
        ResultHeapCharge a = new ResultHeapCharge(breaker, "qa", 1.0);
        ResultHeapCharge b = new ResultHeapCharge(breaker, "qb", 1.0);

        a.charge(700);
        assertEquals(700, breaker.getUsed());
        // b's 400 would push the shared total to 1100 > 1000 → trips, even though b alone is small.
        expectThrows(CircuitBreakingException.class, () -> b.charge(400));
        assertEquals("failed charge left the shared counter at a's contribution", 700, breaker.getUsed());

        // a completing frees room for b.
        a.close();
        assertEquals(0, breaker.getUsed());
        b.charge(400);
        assertEquals(400, breaker.getUsed());
        b.close();
        assertEquals(0, breaker.getUsed());
    }

    /** A null breaker (startup race / no registered breaker) makes every method a no-op. */
    public void testNullBreakerIsFullyNoop() {
        ResultHeapCharge charge = new ResultHeapCharge(null, "q10", 1.5);
        charge.charge(1000); // no breaker → no accounting, no NPE
        assertEquals(0, charge.chargedBytes());
        charge.shrinkTo(10); // safe
        charge.close(); // safe
    }

    /**
     * Cancel-race safety net: a charge registered on {@link QueryContext#onClose} is released when the
     * context closes — the release path that fires from every {@code QueryExecution} terminal, incl.
     * the cancel races where the user ActionListener terminal is never delivered (the leak this fixes).
     */
    public void testContextOnCloseReleasesCharge() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q-ctx", 1.0);
        charge.charge(500);
        assertEquals(500, breaker.getUsed());

        QueryContext ctx = QueryContext.forTest(new org.opensearch.analytics.planner.dag.QueryDAG("q-ctx", mockRootStage()), newTask());
        ctx.onClose(org.opensearch.common.lease.Releasables.releaseOnce(charge));

        // Simulate a terminal where the listener chain never released (cancel race): only close fires.
        ctx.close();
        assertEquals("context close must release the stranded charge", 0, breaker.getUsed());
        assertEquals(0, charge.chargedBytes());
    }

    /** onClose registered AFTER the context is already closed releases inline (never stranded). */
    public void testContextOnCloseAfterClosedReleasesInline() {
        CircuitBreaker breaker = requestBreaker(1_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q-ctx2", 1.0);
        charge.charge(300);

        QueryContext ctx = QueryContext.forTest(new org.opensearch.analytics.planner.dag.QueryDAG("q-ctx2", mockRootStage()), newTask());
        ctx.close();
        ctx.onClose(org.opensearch.common.lease.Releasables.releaseOnce(charge)); // already closed → inline
        assertEquals(0, breaker.getUsed());
    }

    private static org.opensearch.analytics.planner.dag.Stage mockRootStage() {
        org.opensearch.analytics.planner.dag.Stage stage = org.mockito.Mockito.mock(org.opensearch.analytics.planner.dag.Stage.class);
        org.mockito.Mockito.when(stage.getStageId()).thenReturn(0);
        org.mockito.Mockito.when(stage.getChildStages()).thenReturn(java.util.List.of());
        return stage;
    }

    private static org.opensearch.analytics.exec.task.AnalyticsQueryTask newTask() {
        return new org.opensearch.analytics.exec.task.AnalyticsQueryTask(
            1L,
            "transport",
            "analytics_query",
            "q-ctx",
            org.opensearch.core.tasks.TaskId.EMPTY_TASK_ID,
            java.util.Map.of(),
            null
        );
    }
}
