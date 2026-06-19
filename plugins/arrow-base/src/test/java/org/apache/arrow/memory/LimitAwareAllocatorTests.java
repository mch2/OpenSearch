/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.memory;

import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies {@link LimitAwareAllocator}'s core fix: on the allocator-limit OOM path,
 * {@code wrapForeignAllocation} calls {@code release0()} exactly once (balancing the importer's
 * retain) and throws {@link CircuitBreakingException}, leaving the allocator with zero outstanding
 * bytes so it closes cleanly — no leak, no double-free.
 *
 * <p>These tests exercise the allocator directly with a fake {@link ForeignAllocation} (no native
 * C Data layer needed), isolating exactly the behavior that the upstream arrow-java bug gets wrong.
 */
public class LimitAwareAllocatorTests extends OpenSearchTestCase {

    private RootAllocator root;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        root.close(); // throws "Memory was leaked" if anything stranded — the leak assertion
        super.tearDown();
    }

    /** A fake foreign allocation that records whether its release callback fired. */
    private static final class FakeForeignAllocation extends ForeignAllocation {
        final AtomicInteger releaseCount = new AtomicInteger();
        private final long addr;

        FakeForeignAllocation(long size, long addr) {
            super(size, addr);
            this.addr = addr;
        }

        @Override
        protected void release0() {
            releaseCount.incrementAndGet();
        }
    }

    /**
     * Over-budget foreign wrap: must throw CircuitBreakingException AND release the foreign
     * allocation exactly once (the upstream bug skips this release, stranding it).
     */
    public void testOverBudgetWrapReleasesOnceAndThrowsCircuitBreaking() {
        long limit = 1024;
        LimitAwareAllocator alloc = LimitAwareAllocator.createChild(root, "test", limit);
        FakeForeignAllocation foreign = new FakeForeignAllocation(/*size*/ 4096, /*addr*/ 0xdead0000L);

        CircuitBreakingException cbe = expectThrows(
            CircuitBreakingException.class,
            () -> alloc.wrapForeignAllocation(foreign)
        );
        assertTrue(cbe.getMessage().contains("analytics buffer limit exceeded"));
        assertEquals("release0 must fire exactly once on the over-budget path", 1, foreign.releaseCount.get());
        assertEquals("nothing should remain allocated after a rejected wrap", 0, alloc.getAllocatedMemory());
        alloc.close(); // must not throw "Memory was leaked"
    }

    /**
     * Partial scenario: one foreign buffer fits and is wrapped (held by an ArrowBuf), a second
     * over-budget wrap is rejected (release0 fires for it). After releasing the first buffer
     * normally, the allocator returns to zero. Proves the rejected wrap's release does NOT
     * double-free the successful buffer and the allocator stays balanced.
     */
    public void testPartialWrapThenRejectStaysBalanced() {
        long limit = 4096;
        LimitAwareAllocator alloc = LimitAwareAllocator.createChild(root, "test", limit);

        FakeForeignAllocation ok = new FakeForeignAllocation(/*size*/ 2048, /*addr*/ 0x1000L);
        ArrowBuf okBuf = alloc.wrapForeignAllocation(ok); // succeeds, holds 2048
        assertEquals(2048, alloc.getAllocatedMemory());
        assertEquals(0, ok.releaseCount.get());

        FakeForeignAllocation tooBig = new FakeForeignAllocation(/*size*/ 4096, /*addr*/ 0x2000L);
        expectThrows(CircuitBreakingException.class, () -> alloc.wrapForeignAllocation(tooBig));
        assertEquals("rejected wrap released its own foreign allocation", 1, tooBig.releaseCount.get());
        assertEquals("rejected wrap must not touch the successful buffer", 0, ok.releaseCount.get());
        assertEquals("only the successful buffer remains", 2048, alloc.getAllocatedMemory());

        okBuf.close(); // normal release of the successful buffer
        assertEquals("ok buffer released exactly once", 1, ok.releaseCount.get());
        assertEquals("allocator back to zero", 0, alloc.getAllocatedMemory());
        alloc.close(); // must not throw
    }

    /** Within-budget wrap succeeds and the buffer behaves normally (no spurious release). */
    public void testWithinBudgetWrapSucceeds() {
        LimitAwareAllocator alloc = LimitAwareAllocator.createChild(root, "test", 8192);
        FakeForeignAllocation foreign = new FakeForeignAllocation(2048, 0x3000L);
        ArrowBuf buf = alloc.wrapForeignAllocation(foreign);
        assertEquals(2048, buf.capacity());
        assertEquals(0, foreign.releaseCount.get());
        buf.close();
        assertEquals(1, foreign.releaseCount.get());
        assertEquals(0, alloc.getAllocatedMemory());
        alloc.close();
    }

    /** Unbounded (limit of zero or less) never rejects. */
    public void testUnboundedNeverRejects() {
        LimitAwareAllocator alloc = LimitAwareAllocator.createChild(root, "test", 0);
        FakeForeignAllocation foreign = new FakeForeignAllocation(1L << 20, 0x4000L);
        ArrowBuf buf = alloc.wrapForeignAllocation(foreign);
        buf.close();
        assertEquals(1, foreign.releaseCount.get());
        alloc.close();
    }

    /**
     * REAL C Data import path: export a multi-field batch, then import it into a LimitAwareAllocator
     * whose limit is exceeded partway through (some buffers wrapped, then OOM). After the failed
     * import, the allocator must hold ZERO bytes and close cleanly — no stranded batch (the bug),
     * no double-free. This reproduces the production partial-import scenario that a single
     * fake-allocation test cannot.
     */
    public void testRealImportPartialFailureLeavesNoLeak() {
        final int rows = 50_000;
        try (org.apache.arrow.vector.VectorSchemaRoot src = buildTwoFieldBatch(root, rows)) {
            org.apache.arrow.c.ArrowArray array = org.apache.arrow.c.ArrowArray.allocateNew(root);
            org.apache.arrow.c.ArrowSchema schema = org.apache.arrow.c.ArrowSchema.allocateNew(root);
            org.apache.arrow.c.Data.exportVectorSchemaRoot(root, src, null, array, schema);

            LimitAwareAllocator consumer = LimitAwareAllocator.createChild(root, "consumer", 256 * 1024);
            try (
                org.apache.arrow.c.CDataDictionaryProvider dict = new org.apache.arrow.c.CDataDictionaryProvider();
                org.apache.arrow.vector.VectorSchemaRoot dst = org.apache.arrow.c.Data.importVectorSchemaRoot(consumer, schema, dict)
            ) {
                expectThrows(
                    RuntimeException.class,
                    () -> org.apache.arrow.c.Data.importIntoVectorSchemaRoot(consumer, array, dst, dict)
                );
            }
            assertEquals("consumer allocator leaked after partial import failure", 0, consumer.getAllocatedMemory());
            consumer.close(); // must not throw "Memory was leaked"
        }
    }

    private static org.apache.arrow.vector.VectorSchemaRoot buildTwoFieldBatch(BufferAllocator allocator, int rows) {
        org.apache.arrow.vector.BigIntVector a = new org.apache.arrow.vector.BigIntVector("a", allocator);
        org.apache.arrow.vector.BigIntVector b = new org.apache.arrow.vector.BigIntVector("b", allocator);
        a.allocateNew(rows);
        b.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            a.set(i, i);
            b.set(i, i * 2L);
        }
        a.setValueCount(rows);
        b.setValueCount(rows);
        return org.apache.arrow.vector.VectorSchemaRoot.of(a, b);
    }
}
