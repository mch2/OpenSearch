/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Tests for {@link QueryState} metrics ownership: {@code metricsFor(stageId)}
 * and {@code allStageMetrics()}.
 *
 * Validates: Requirements 3.1
 */
public class QueryStateMetricsTests extends OpenSearchTestCase {

    /**
     * {@code metricsFor(id)} returns the same instance on repeated calls for
     * the same stageId, and different instances for different stageIds.
     *
     * Validates: Requirements 3.1
     */
    public void testMetricsForIsStableAcrossLookups() {
        QueryState state = new QueryState();

        StageMetrics first = state.metricsFor(0);
        StageMetrics second = state.metricsFor(0);
        assertSame("metricsFor(0) must return the same instance on repeated calls", first, second);

        StageMetrics other = state.metricsFor(1);
        assertNotSame("metricsFor(0) and metricsFor(1) must return different instances", first, other);
    }

    /**
     * {@code allStageMetrics()} returns a snapshot containing all stages that
     * have been looked up, and the returned map is unmodifiable.
     *
     * Validates: Requirements 3.1
     */
    public void testAllStageMetricsReturnsSnapshot() {
        QueryState state = new QueryState();

        state.metricsFor(0);
        state.metricsFor(1);
        state.metricsFor(2);

        Map<Integer, StageMetrics> snapshot = state.allStageMetrics();
        assertEquals("allStageMetrics must contain all three stages", 3, snapshot.size());
        assertTrue("allStageMetrics must contain stage 0", snapshot.containsKey(0));
        assertTrue("allStageMetrics must contain stage 1", snapshot.containsKey(1));
        assertTrue("allStageMetrics must contain stage 2", snapshot.containsKey(2));

        // The returned map must be unmodifiable
        expectThrows(UnsupportedOperationException.class, () -> snapshot.put(99, new StageMetrics(99)));
    }
}
