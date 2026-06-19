/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.memory;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A byte-limit admission gate for a {@link BufferAllocator}, modeled on Elasticsearch ES|QL's
 * {@code BlockFactory}/{@code CircuitBreaker} pattern: reserve-then-allocate, never a hard limit
 * inside the allocator.
 *
 * <p>Lives in {@code org.apache.arrow.memory} so it can attach to a stock child allocator built with
 * {@code parent.newChildAllocator(name, listener, 0, Long.MAX_VALUE)} — the allocator itself stays
 * UNBOUNDED.
 *
 * <h2>Why reserve-before-allocate, not a per-buffer throw</h2>
 * arrow-java performs indivisible multi-buffer operations — a C-Data export ({@code SchemaExporter}
 * allocating field-name/format buffers then per-child structs) and a C-Data import
 * ({@code importBuffer} → {@code unsafeAssociateAllocation} retaining one buffer per field). None of
 * these clean up if an allocation midway throws. Enforcing the cap per-buffer (whether via the
 * allocator's {@code maxAllocation} or this listener's {@code onPreAllocation}) therefore strands the
 * buffers already allocated/retained — a leak that only appears once the limit is small enough to be
 * hit mid-operation.
 *
 * <p>Instead, callers {@link #reserve} the whole operation's footprint up-front. {@code reserve}
 * either admits the full amount atomically or throws {@link CircuitBreakingException} having reserved
 * nothing — so the operation only ever starts when it fully fits, and can never trip the limit
 * partway. The matching {@link #release} is called when the operation's buffers are freed. The
 * {@link AllocationListener} hooks are NOT used to enforce the limit (they would reintroduce the
 * mid-operation throw); they only maintain {@link #actualInUse} for diagnostics.
 *
 * <p>{@code limitBytes <= 0} disables the gate (unbounded). Adjustable at runtime via
 * {@link #updateLimit(long)}.
 */
public final class BufferLimitAllocationListener implements AllocationListener {

    private final String name;
    private final AtomicLong limitBytes;
    /** Bytes admitted by {@link #reserve} and not yet {@link #release}d — the gate counter. */
    private final AtomicLong reserved = new AtomicLong();
    /** Real allocator bytes (onAllocation/onRelease). Diagnostics only; not the gate. */
    private final AtomicLong actualInUse = new AtomicLong();

    public BufferLimitAllocationListener(String name, long limitBytes) {
        this.name = name;
        this.limitBytes = new AtomicLong(limitBytes <= 0 ? Long.MAX_VALUE : limitBytes);
    }

    /** Update the cap at runtime. {@code <= 0} → unbounded. */
    public void updateLimit(long newLimitBytes) {
        limitBytes.set(newLimitBytes <= 0 ? Long.MAX_VALUE : newLimitBytes);
    }

    /**
     * Atomically admit {@code bytes} against the limit, or throw {@link CircuitBreakingException}
     * having reserved nothing. Call ONCE before starting a multi-buffer C-Data export/import with the
     * operation's full estimated footprint; call {@link #release} with the same amount when its
     * buffers are freed. This is the ES|QL {@code addEstimateBytesAndMaybeBreak} shape.
     */
    public void reserve(long bytes) {
        if (bytes <= 0) {
            return;
        }
        long limit = limitBytes.get();
        if (limit == Long.MAX_VALUE) {
            return; // unbounded
        }
        // CAS loop: admit only if it keeps us within the limit; reserve nothing on rejection.
        while (true) {
            long current = reserved.get();
            long next = current + bytes;
            if (next > limit) {
                throw new CircuitBreakingException(
                    "analytics buffer limit exceeded for ["
                        + name
                        + "]: requested="
                        + bytes
                        + "B + reserved="
                        + current
                        + "B would exceed limit="
                        + limit
                        + "B",
                    bytes,
                    limit,
                    CircuitBreaker.Durability.TRANSIENT
                );
            }
            if (reserved.compareAndSet(current, next)) {
                return;
            }
            // lost the race — retry with the updated value
        }
    }

    /** Release a prior {@link #reserve}. Must balance each successful reserve exactly once. */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        reserved.addAndGet(-bytes);
    }

    // ── AllocationListener: diagnostics only, never enforce the limit ──

    @Override
    public void onAllocation(long size) {
        actualInUse.addAndGet(size);
    }

    @Override
    public void onRelease(long size) {
        actualInUse.addAndGet(-size);
    }

    /** Bytes currently reserved (the gate counter). */
    public long reserved() {
        return reserved.get();
    }

    /** Real allocator bytes in use (diagnostics). */
    public long actualInUse() {
        return actualInUse.get();
    }
}
