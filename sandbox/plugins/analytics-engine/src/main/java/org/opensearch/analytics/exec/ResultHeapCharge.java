/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Per-query reservation of heap against the shared, node-level {@code analytics_query}
 * {@link CircuitBreaker}, charged incrementally as result batches are materialized into Java rows.
 *
 * <p>This is the cross-request memory guard the pure 10k-row cap in {@code RowProducingSink} cannot
 * provide: every query charges the <em>same</em> breaker instance, so N concurrent wide-row results
 * collectively trip it (and its parent real-memory breaker) rather than each staying under an
 * isolated per-query row count. The breaker's accumulated {@code used} bytes are the shared state
 * that encodes "how much heap the already-admitted, currently-reading queries are holding."
 *
 * <p>Usage mirrors {@code AggregatorBase}/{@code QueryPhaseResultConsumer}: {@link #charge(long)} to
 * reserve (may throw {@link CircuitBreakingException} → HTTP 429), keep a running total, and
 * {@link #release()} exactly that total on any terminal. {@link #release()} is idempotent.
 *
 * <p>A {@code null} breaker (startup race, or a test/embedding without a registered breaker) makes
 * every method a no-op, degrading to the prior unaccounted behavior rather than failing the query.
 *
 * @opensearch.internal
 */
public final class ResultHeapCharge {

    private final CircuitBreaker breaker;
    private final String label;
    private long charged;      // guarded by this
    private boolean released;  // guarded by this

    public ResultHeapCharge(CircuitBreaker breaker, String queryId) {
        this.breaker = breaker;
        this.label = "<analytics_result[" + queryId + "]>";
    }

    /**
     * Pre-emptively reserves {@code bytes} against the breaker <em>before</em> the corresponding heap
     * is allocated. Throws {@link CircuitBreakingException} (mapped to HTTP 429 by the REST layer) if
     * the child or parent real-memory limit would be exceeded — in which case nothing is added (the
     * breaker rolls back its own increment) and the running total is unchanged, so {@link #release()}
     * stays correct. Mirrors {@code BigArrays.adjustBreaker}'s charge-before-allocate.
     */
    public synchronized void charge(long bytes) {
        if (breaker == null || bytes <= 0 || released) {
            return;
        }
        breaker.addEstimateBytesAndMaybeBreak(bytes, label); // throws CircuitBreakingException on breach
        charged += bytes;
    }

    /**
     * Trues up a prior {@link #charge(long)} estimate to the measured real size, exactly like
     * {@code QueryPhaseResultConsumer}'s reserve-estimate-then-correct-to-real. A positive delta
     * (real &gt; estimate) is added <em>without</em> a break check — the heap is already allocated, so
     * refusing it here would only desync accounting; the parent real-memory breaker is the backstop.
     * A negative delta (estimate was high) releases the difference. No-op once released.
     */
    public synchronized void adjust(long deltaBytes) {
        if (breaker == null || deltaBytes == 0 || released) {
            return;
        }
        breaker.addWithoutBreaking(deltaBytes);
        charged += deltaBytes;
    }

    /** Releases exactly what was charged. Idempotent — safe to call on every terminal path. */
    public synchronized void release() {
        if (breaker == null || released) {
            return;
        }
        released = true;
        if (charged > 0) {
            breaker.addWithoutBreaking(-charged);
            charged = 0;
        }
    }

    /** Current reserved bytes (test/observability). */
    public synchronized long chargedBytes() {
        return charged;
    }
}
