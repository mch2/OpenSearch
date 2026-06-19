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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A bounded Arrow child allocator that is safe to import C Data foreign buffers into.
 *
 * <p>Lives in {@code org.apache.arrow.memory} so it can call the {@code protected}
 * {@link ForeignAllocation#release0()} and the {@code protected} {@link BaseAllocator}
 * constructor.
 *
 * <h2>The bug this fixes</h2>
 * arrow-java's {@link BaseAllocator#wrapForeignAllocation} throws {@code OutOfMemoryException} on
 * the allocator-limit path BEFORE its try/catch, so it never calls {@code allocation.release0()}.
 * The C Data importer ({@code ReferenceCountedArrowArray.unsafeAssociateAllocation}) does
 * {@code retain()} THEN calls {@code wrapForeignAllocation}, so that throw strands the retain and
 * the imported foreign batch (e.g. a DataFusion record batch) is never released — a leak. Unfixed
 * through arrow-java 19.0.0.
 *
 * <h2>Why this is safe (no double-free)</h2>
 * We override {@code wrapForeignAllocation} to call {@code allocation.release0()} on the OOM path.
 * That is exactly the ONE decrement the upstream code skips — it completes the importer's existing
 * refcount chain to 0 (each successfully-wrapped buffer is released by the importer's
 * {@code BufferImportTypeVisitor.close()}, and the moved-in {@code ownedArray} by
 * {@code ArrayImporter.importArray}'s finally). It is NOT an independent second release (which is
 * what double-frees), so it is safe even on a partial import. The native producer release callback
 * fires exactly once.
 */
public final class LimitAwareAllocator extends BaseAllocator {

    private final String displayName;

    /**
     * TEMP DIAGNOSTIC: per-buffer allocation stack capture for the reduce residual-leak hunt. Keyed by
     * ArrowBuf identity (the memory address). On close() any entry still present is a leaked buffer —
     * we print its allocation stack so the exact alloc site is known. Self-contained: does not depend
     * on Arrow's debug-allocator flag (which doesn't propagate to this plugin classloader).
     */
    private static final boolean TRACE_ALLOCS = Boolean.getBoolean("analytics.alloc.trace");
    private final Map<Long, Throwable> allocStacks = TRACE_ALLOCS ? new ConcurrentHashMap<>() : null;

    private LimitAwareAllocator(BaseAllocator parent, String name, long maxAllocation) {
        super(parent, name, configBuilder().maxAllocation(maxAllocation).build());
        this.displayName = name;
    }

    @Override
    public ArrowBuf buffer(long initialRequestSize, BufferManager manager) {
        ArrowBuf buf = super.buffer(initialRequestSize, manager);
        // Track only SMALL allocations: the reduce residual leak is ~298B; data batches are ~131KB.
        // Filtering to small buffers isolates the leak signal from the (correctly-freed) batch noise.
        if (allocStacks != null && buf != null && buf.capacity() <= 4096) {
            allocStacks.put(buf.memoryAddress(), new Throwable("alloc " + buf.capacity() + "B"));
        }
        return buf;
    }

    @Override
    public void close() {
        if (allocStacks != null) {
            long held = getAllocatedMemory();
            if (held > 0) {
                org.apache.logging.log4j.LogManager.getLogger(LimitAwareAllocator.class)
                    .warn("[alloc-trace] {} closing with {}B held; {} tracked buffers — dumping alloc stacks", displayName, held, allocStacks.size());
                for (Map.Entry<Long, Throwable> e : allocStacks.entrySet()) {
                    org.apache.logging.log4j.LogManager.getLogger(LimitAwareAllocator.class)
                        .warn("[alloc-trace] leaked buffer @" + Long.toHexString(e.getKey()), e.getValue());
                }
            }
            allocStacks.clear();
        }
        super.close();
    }

    /**
     * Create a bounded import allocator as a child of {@code parent}. {@code limitBytes <= 0} means
     * unbounded ({@link Long#MAX_VALUE}).
     *
     * <p>NOTE: the child is intentionally NOT inserted into the parent's private {@code childAllocators}
     * registry (it's inaccessible). The {@code BaseAllocator} constructor still wires the parent/root
     * links so accounting rolls up correctly. The only consequence is that Arrow's DEBUG-mode
     * close-time child validator ({@code childClosed}) would not find it — so the Arrow debug
     * allocator ({@code -Darrow.memory.debug.allocator=true}) must remain OFF in production, which it
     * is by default. In non-DEBUG mode {@code childClosed} is a harmless map removal.
     */
    public static LimitAwareAllocator createChild(BufferAllocator parent, String name, long limitBytes) {
        BaseAllocator base = (BaseAllocator) parent;
        long max = limitBytes <= 0 ? Long.MAX_VALUE : limitBytes;
        return new LimitAwareAllocator(base, name, max);
    }

    /** Update the cap at runtime. {@code <= 0} → unbounded. */
    public void updateLimit(long limitBytes) {
        setLimit(limitBytes <= 0 ? Long.MAX_VALUE : limitBytes);
    }

    @Override
    public ArrowBuf wrapForeignAllocation(ForeignAllocation allocation) {
        try {
            return super.wrapForeignAllocation(allocation);
        } catch (OutOfMemoryException oom) {
            // Upstream skipped release0() on this throw path — call it now to balance the importer's
            // retain() (the single missing decrement). Safe: not an independent release.
            try {
                allocation.release0();
            } catch (Throwable ignore) {
                // best-effort; never mask the limit error
            }
            throw new CircuitBreakingException(
                "analytics buffer limit exceeded for ["
                    + displayName
                    + "]: requested="
                    + allocation.getSize()
                    + "B would exceed limit="
                    + getLimit()
                    + "B",
                allocation.getSize(),
                getLimit(),
                CircuitBreaker.Durability.TRANSIENT
            );
        }
    }
}
