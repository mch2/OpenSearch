/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for the {@link LocalStageContext} interface contract.
 * Uses a simple test implementation to verify that the interface compiles
 * and that a backend can implement it correctly.
 */
public class LocalStageContextTests extends OpenSearchTestCase {

    /**
     * Minimal test implementation of {@link LocalStageContext} that records
     * per-child sinks and fires the asyncFinalize listener immediately.
     */
    static class TestLocalStageContext implements LocalStageContext {
        private final Map<Integer, ExchangeSink> childSinks = new ConcurrentHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        void registerChildSink(int childStageId, ExchangeSink sink) {
            childSinks.put(childStageId, sink);
        }

        @Override
        public ExchangeSink sinkFor(int childStageId) {
            ExchangeSink sink = childSinks.get(childStageId);
            if (sink == null) {
                throw new IllegalArgumentException("No sink registered for child stage " + childStageId);
            }
            return sink;
        }

        @Override
        public void asyncFinalize(ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }

        boolean isClosed() {
            return closed.get();
        }
    }

    /**
     * Minimal no-op ExchangeSink for testing.
     */
    static class RecordingSink implements ExchangeSink {
        private int feedCount = 0;

        @Override
        public void feed(org.apache.arrow.vector.VectorSchemaRoot batch) {
            feedCount++;
        }

        @Override
        public void close() {}

        @Override
        public Iterable<Object[]> readResult() {
            return java.util.List.of();
        }

        @Override
        public long getRowCount() {
            return feedCount;
        }

        @Override
        public Object getValueAt(String column, int rowIndex) {
            return null;
        }

        int getFeedCount() {
            return feedCount;
        }
    }

    public void testSinkForReturnsRegisteredSink() {
        TestLocalStageContext ctx = new TestLocalStageContext();
        RecordingSink sink = new RecordingSink();
        ctx.registerChildSink(42, sink);

        ExchangeSink returned = ctx.sinkFor(42);
        assertSame(sink, returned);
    }

    public void testSinkForThrowsOnUnknownChild() {
        TestLocalStageContext ctx = new TestLocalStageContext();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ctx.sinkFor(99));
        assertTrue(ex.getMessage().contains("99"));
    }

    public void testAsyncFinalizeFiresListener() throws Exception {
        TestLocalStageContext ctx = new TestLocalStageContext();
        ctx.registerChildSink(1, new RecordingSink());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean(false);

        ctx.asyncFinalize(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });

        assertTrue("asyncFinalize listener should have been called", latch.await(5, TimeUnit.SECONDS));
        assertTrue("asyncFinalize should have succeeded", success.get());
        assertNull("asyncFinalize should not have failed", failure.get());
    }

    public void testCloseIsIdempotent() {
        TestLocalStageContext ctx = new TestLocalStageContext();

        assertFalse(ctx.isClosed());
        ctx.close();
        assertTrue(ctx.isClosed());
        // Second close is a no-op — no exception
        ctx.close();
        assertTrue(ctx.isClosed());
    }
}
