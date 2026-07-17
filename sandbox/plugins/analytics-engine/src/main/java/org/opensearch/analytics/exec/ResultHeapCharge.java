/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Per-query reservation of heap against the shared, node-level {@code request}
 * {@link CircuitBreaker} — the same breaker {@code QueryPhaseResultConsumer} and {@code
 * AggregatorBase} charge — used to gate analytics result materialization.
 *
 * <p>This is the cross-request memory guard the pure 10k-row cap in {@code RowProducingSink} cannot
 * provide: every query charges the <em>same</em> breaker instance, so N concurrent wide-row results
 * collectively trip it (and its parent real-memory breaker) rather than each staying under an
 * isolated per-query row count. The breaker's accumulated {@code used} bytes are the shared state
 * that encodes "how much heap the already-admitted, currently-reading queries are holding."
 *
 * <p>Lifecycle mirrors {@code AggregatorBase}/{@code QueryPhaseResultConsumer}: {@link #charge(long)}
 * once with the pessimistic worst-case footprint <em>before</em> dispatch (may throw {@link
 * CircuitBreakingException} → HTTP 429), then {@link #shrinkTo(long)} once the true materialized size
 * is known to hand back the over-reservation, and finally {@link #close()} on any terminal to release
 * the remainder. Implements {@link Releasable} so it composes with {@code Releasables.releaseOnce} /
 * {@code ActionListener.runAfter}. {@link #close()} is idempotent.
 *
 * <p>The charged native estimate is scaled by {@code heapExpansionFactor} to account for the
 * Java-object expansion over the Arrow native footprint (and the downstream response copy). The
 * factor is applied symmetrically in {@link #charge} and {@link #shrinkTo} so the reservation tracks
 * the same units throughout.
 *
 * <p>A {@code null} breaker (startup race, or a test/embedding without a registered breaker) makes
 * every method a no-op, degrading to the prior unaccounted behavior rather than failing the query.
 *
 * @opensearch.internal
 */
public final class ResultHeapCharge implements Releasable {

    private final CircuitBreaker breaker;
    private final String label;
    private final double heapExpansionFactor;
    private long charged;      // guarded by this; expanded (heap) bytes currently reserved
    private boolean released;  // guarded by this

    public ResultHeapCharge(CircuitBreaker breaker, String queryId, double heapExpansionFactor) {
        this.breaker = breaker;
        this.label = "<analytics_result[" + queryId + "]>";
        this.heapExpansionFactor = heapExpansionFactor;
    }

    /**
     * Pre-emptively reserves the heap-expanded size of {@code nativeBytes} against the breaker
     * <em>before</em> the corresponding heap is allocated. Throws {@link CircuitBreakingException}
     * (mapped to HTTP 429 by the REST layer) if the child or parent real-memory limit would be
     * exceeded — in which case nothing is added (the breaker rolls back its own increment) and the
     * running total is unchanged, so {@link #close()} stays correct. Mirrors {@code
     * BigArrays.adjustBreaker}'s charge-before-allocate.
     */
    public synchronized void charge(long nativeBytes) {
        if (breaker == null || nativeBytes <= 0 || released) {
            return;
        }
        long expanded = expand(nativeBytes);
        breaker.addEstimateBytesAndMaybeBreak(expanded, label); // throws CircuitBreakingException on breach
        charged += expanded;
    }

    /**
     * Relaxes the reservation down to the heap-expanded size of {@code actualNativeBytes}, releasing
     * the difference between the upfront worst-case charge and the actual materialized footprint. This
     * only ever <em>shrinks</em>: if the actual would be at or above what is currently charged (the
     * worst-case estimate was not an over-count), it does nothing — the upfront estimate is the
     * ceiling and we never grow a reservation after admission.
     */
    public synchronized void shrinkTo(long actualNativeBytes) {
        if (breaker == null || released) {
            return;
        }
        long target = actualNativeBytes <= 0 ? 0 : expand(actualNativeBytes);
        if (target >= charged) {
            return; // never grow
        }
        breaker.addWithoutBreaking(target - charged); // negative → release
        charged = target;
    }

    /** Releases exactly what remains reserved. Idempotent — safe to call on every terminal path. */
    @Override
    public synchronized void close() {
        if (breaker == null || released) {
            return;
        }
        released = true;
        if (charged > 0) {
            breaker.addWithoutBreaking(-charged);
            charged = 0;
        }
    }

    /** Current reserved (heap-expanded) bytes (test/observability). */
    public synchronized long chargedBytes() {
        return charged;
    }

    private long expand(long nativeBytes) {
        double expanded = nativeBytes * heapExpansionFactor;
        return expanded >= (double) Long.MAX_VALUE ? Long.MAX_VALUE : (long) expanded;
    }
}
