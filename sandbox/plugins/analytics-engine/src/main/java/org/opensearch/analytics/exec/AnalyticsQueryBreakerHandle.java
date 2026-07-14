/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.core.common.breaker.CircuitBreaker;

/**
 * Mutable holder for the node-level {@code analytics_query} {@link CircuitBreaker}.
 *
 * <p>The breaker is a per-node singleton constructed by the framework and handed to
 * {@code AnalyticsPlugin#setCircuitBreaker(CircuitBreaker)} at node startup — which happens
 * <em>after</em> {@code createComponents(...)} builds the Guice graph. {@link org.opensearch.analytics.exec.DefaultPlanExecutor}
 * is {@code @Inject}-constructed from that graph, so it cannot receive the breaker directly. This
 * handle is returned from {@code createComponents} (and thus injectable), and the plugin populates
 * it in {@code setCircuitBreaker}. Mirrors {@link CoordinatorAllocatorHandle}'s ownership seam.
 *
 * <p>Because charging only happens at query result-materialization time — long after node startup —
 * the breaker is always set by the time it is read. {@link #breaker()} tolerates a null breaker
 * (returns {@code null}) so a query issued in the startup race, or in a test/embedding without a
 * registered breaker, degrades to no accounting rather than an NPE.
 *
 * @opensearch.internal
 */
public final class AnalyticsQueryBreakerHandle {

    private volatile CircuitBreaker breaker;

    /** Sets the node breaker. Called once by the plugin's {@code setCircuitBreaker}. */
    public void setBreaker(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /** The node {@code analytics_query} breaker, or {@code null} if not yet registered. */
    public CircuitBreaker breaker() {
        return breaker;
    }
}
