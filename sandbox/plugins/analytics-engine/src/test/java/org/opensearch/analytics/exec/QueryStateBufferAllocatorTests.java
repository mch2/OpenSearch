/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the per-query {@link BufferAllocator} on {@link QueryState}.
 *
 * Validates: design.md "Required additions to existing types"
 */
public class QueryStateBufferAllocatorTests extends OpenSearchTestCase {

    /**
     * The allocator is created lazily on first access and returns the same
     * instance on subsequent calls. Closing it releases resources.
     */
    public void testAllocatorIsLazyAndClosed() {
        QueryState state = new QueryState();

        // First access creates the allocator
        BufferAllocator alloc1 = state.bufferAllocator();
        assertNotNull("bufferAllocator() should return a non-null allocator", alloc1);

        // Second access returns the same instance
        BufferAllocator alloc2 = state.bufferAllocator();
        assertSame("bufferAllocator() should return the same instance on repeated calls", alloc1, alloc2);

        // Allocator should be usable
        byte[] buf = new byte[64];
        org.apache.arrow.memory.ArrowBuf arrowBuf = alloc1.buffer(64);
        assertNotNull(arrowBuf);
        arrowBuf.close();

        // Close the allocator
        state.closeBufferAllocator();

        // After close, allocating should throw
        try {
            alloc1.buffer(64);
            fail("Expected exception after closing allocator");
        } catch (Exception e) {
            // expected — allocator is closed
        }
    }

    /**
     * closeBufferAllocator is safe to call even if the allocator was never created.
     */
    public void testCloseWithoutCreatingAllocator() {
        QueryState state = new QueryState();
        // Should not throw
        state.closeBufferAllocator();
    }
}
