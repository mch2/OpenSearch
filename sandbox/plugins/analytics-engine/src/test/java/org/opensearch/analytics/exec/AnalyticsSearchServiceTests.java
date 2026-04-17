/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.arrow.flight.transport.ArrowFlightChannel;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportChannel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests for {@link AnalyticsSearchService#executeFragmentStreaming}.
 *
 * <p>Covers the data-node-side streaming path: per-batch
 * {@code channel.sendResponseBatch}, terminal {@code completeStream}, and
 * the post-completion {@code awaitDrained} barrier. The transport channel
 * is a mock implementing both {@link TransportChannel} and
 * {@link ArrowFlightChannel}, so {@link ArrowFlightChannel#from(TransportChannel)}
 * resolves it without walking a real wrapper chain.
 */
public class AnalyticsSearchServiceTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "mock-backend";

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        if (allocator != null) allocator.close();
        super.tearDown();
    }

    public void testStreamingHappyPath() throws Exception {
        List<VectorSchemaRoot> batches = List.of(newIntRoot("x", 3), newIntRoot("x", 2));
        RecordingChannel channel = new RecordingChannel(allocator);
        IndexShard shard = mockShard();
        AnalyticsSearchService svc = new AnalyticsSearchService(Map.of(BACKEND_ID, backendEmitting(batches)));

        try {
            svc.executeFragmentStreaming(request(), shard, null, channel);
        } finally {
            closeAll(batches);
        }

        assertEquals("one sendResponseBatch per engine batch", 2, channel.batchCount.get());
        assertTrue("completeStream was called", channel.completeStreamCalled.get());
        assertTrue("awaitDrained was called after completeStream", channel.awaitDrainedCalled.get());
        assertTrue(
            "awaitDrained must come AFTER completeStream to drain any pending async work",
            channel.awaitDrainedAfterComplete.get()
        );
    }

    public void testMissingCompositeEngineThrowsIllegalState() {
        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(null);
        when(shard.shardId()).thenReturn(new ShardId(new Index("idx", "uuid"), 0));
        AnalyticsSearchService svc = new AnalyticsSearchService(Map.of());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> svc.executeFragmentStreaming(request(), shard, null, new RecordingChannel(allocator))
        );
        assertTrue(e.getMessage().contains("No CompositeEngine"));
    }

    public void testNoBackendMatchThrowsIllegalArgument() throws Exception {
        IndexShard shard = mockShard();
        AnalyticsSearchService svc = new AnalyticsSearchService(Map.of());  // empty — no match

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> svc.executeFragmentStreaming(request(), shard, null, new RecordingChannel(allocator))
        );
        assertTrue(e.getMessage().contains("No plan alternative matches available backends"));
    }

    public void testEngineFailureSurfacesAsSendResponseError() throws Exception {
        RuntimeException boom = new RuntimeException("engine boom");
        RecordingChannel channel = new RecordingChannel(allocator);
        IndexShard shard = mockShard();
        AnalyticsSearchService svc = new AnalyticsSearchService(Map.of(BACKEND_ID, backendThrowing(boom)));

        svc.executeFragmentStreaming(request(), shard, null, channel);

        assertEquals("no batches sent", 0, channel.batchCount.get());
        assertFalse("completeStream NOT called on error", channel.completeStreamCalled.get());
        assertSame("error routed to sendResponse(Exception)", boom, channel.sentError.get());
    }

    public void testAllocatorFromChannelPropagatesToEngineCtx() throws Exception {
        List<VectorSchemaRoot> batches = List.of(newIntRoot("x", 1));
        RecordingChannel channel = new RecordingChannel(allocator);
        IndexShard shard = mockShard();
        AllocatorCapturingBackend backend = new AllocatorCapturingBackend(batches);
        AnalyticsSearchService svc = new AnalyticsSearchService(Map.of(BACKEND_ID, backend));

        try {
            svc.executeFragmentStreaming(request(), shard, null, channel);
        } finally {
            closeAll(batches);
        }

        assertSame(
            "ExecutionContext allocator is the Flight channel allocator (for zero-copy transfer in one tree)",
            channel.getAllocator(),
            backend.capturedAllocator.get()
        );
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private FragmentExecutionRequest request() {
        return new FragmentExecutionRequest(
            "q-test",
            0,
            new ShardId(new Index("idx", UUID.randomUUID().toString()), 0),
            List.of(new FragmentExecutionRequest.PlanAlternative(BACKEND_ID, new byte[0]))
        );
    }

    private IndexShard mockShard() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        DataFormatAwareEngine engine = mock(DataFormatAwareEngine.class);
        @SuppressWarnings("unchecked")
        GatedCloseable<Reader> gated = mock(GatedCloseable.class);
        Reader reader = mock(Reader.class);
        when(gated.get()).thenReturn(reader);
        when(engine.acquireReader()).thenReturn(gated);
        when(shard.getCompositeEngine()).thenReturn(engine);
        when(shard.shardId()).thenReturn(new ShardId(new Index("idx", "uuid"), 0));
        return shard;
    }

    private AnalyticsSearchBackendPlugin backendEmitting(List<VectorSchemaRoot> batches) {
        AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
        @SuppressWarnings("unchecked")
        SearchExecEngine<ExecutionContext, EngineResultStream> engine = mock(SearchExecEngine.class);
        EngineResultStream stream = new StaticEngineResultStream(batches);
        try {
            when(backend.createSearchExecEngine(any(ExecutionContext.class))).thenReturn(engine);
            when(engine.execute(any(ExecutionContext.class))).thenReturn(stream);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
        return backend;
    }

    private AnalyticsSearchBackendPlugin backendThrowing(RuntimeException cause) {
        AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
        @SuppressWarnings("unchecked")
        SearchExecEngine<ExecutionContext, EngineResultStream> engine = mock(SearchExecEngine.class);
        try {
            when(backend.createSearchExecEngine(any(ExecutionContext.class))).thenReturn(engine);
            when(engine.execute(any(ExecutionContext.class))).thenThrow(cause);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
        return backend;
    }

    private void closeAll(List<VectorSchemaRoot> roots) {
        for (VectorSchemaRoot r : roots) r.close();
    }

    private VectorSchemaRoot newIntRoot(String name, int rowCount) {
        Field f = new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(f)), allocator);
        IntVector v = (IntVector) root.getVector(name);
        v.allocateNew();
        for (int i = 0; i < rowCount; i++) v.setSafe(i, i);
        v.setValueCount(rowCount);
        root.setRowCount(rowCount);
        return root;
    }

    // ── Test doubles ────────────────────────────────────────────────────

    /**
     * Mock {@link TransportChannel} that is also an {@link ArrowFlightChannel},
     * so {@link ArrowFlightChannel#from(TransportChannel)} resolves to it
     * directly without walking a real wrapper chain.
     */
    static final class RecordingChannel implements TransportChannel, ArrowFlightChannel {

        private final BufferAllocator flightAllocator;
        final AtomicInteger batchCount = new AtomicInteger();
        final AtomicBoolean completeStreamCalled = new AtomicBoolean();
        final AtomicBoolean awaitDrainedCalled = new AtomicBoolean();
        final AtomicBoolean awaitDrainedAfterComplete = new AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Exception> sentError = new java.util.concurrent.atomic.AtomicReference<>();

        RecordingChannel(BufferAllocator flightAllocator) {
            this.flightAllocator = flightAllocator;
        }

        @Override public BufferAllocator getAllocator() { return flightAllocator; }

        @Override public void awaitDrained() {
            awaitDrainedCalled.set(true);
            awaitDrainedAfterComplete.set(completeStreamCalled.get());
        }

        @Override public String getProfileName() { return "test"; }
        @Override public String getChannelType() { return "test-channel"; }
        @Override public void sendResponse(TransportResponse response) { }
        @Override public void sendResponse(Exception exception) { sentError.set(exception); }
        @Override public void sendResponseBatch(TransportResponse response) { batchCount.incrementAndGet(); }
        @Override public void completeStream() { completeStreamCalled.set(true); }
    }

    /** {@link EngineResultStream} that emits a fixed list of pre-built VSRs, one per batch. */
    static final class StaticEngineResultStream implements EngineResultStream {
        private final List<VectorSchemaRoot> batches;

        StaticEngineResultStream(List<VectorSchemaRoot> batches) { this.batches = batches; }

        @Override public Iterator<EngineResultBatch> iterator() {
            return new Iterator<>() {
                int i = 0;
                @Override public boolean hasNext() { return i < batches.size(); }
                @Override public EngineResultBatch next() {
                    if (hasNext() == false) throw new NoSuchElementException();
                    VectorSchemaRoot root = batches.get(i++);
                    return new EngineResultBatch() {
                        @Override public List<String> getFieldNames() {
                            List<String> names = new ArrayList<>();
                            for (Field f : root.getSchema().getFields()) names.add(f.getName());
                            return names;
                        }
                        @Override public int getRowCount() { return root.getRowCount(); }
                        @Override public Object getFieldValue(String fieldName, int rowIndex) {
                            return root.getVector(fieldName).getObject(rowIndex);
                        }
                        @Override public VectorSchemaRoot getArrowRoot() { return root; }
                    };
                }
            };
        }

        @Override public void close() { }
    }

    /** Backend whose engine captures the allocator set on the {@link ExecutionContext}. */
    static final class AllocatorCapturingBackend implements AnalyticsSearchBackendPlugin {
        final java.util.concurrent.atomic.AtomicReference<BufferAllocator> capturedAllocator = new java.util.concurrent.atomic.AtomicReference<>();
        private final List<VectorSchemaRoot> batches;

        AllocatorCapturingBackend(List<VectorSchemaRoot> batches) { this.batches = batches; }

        @Override public String name() { return BACKEND_ID; }

        @Override
        public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
            capturedAllocator.set(ctx.getAllocator());
            return new SearchExecEngine<>() {
                @Override public void prepare(ExecutionContext requestContext) { }
                @Override public EngineResultStream execute(ExecutionContext requestContext) {
                    return new StaticEngineResultStream(batches);
                }
                @Override public void close() { }
            };
        }
    }
}
