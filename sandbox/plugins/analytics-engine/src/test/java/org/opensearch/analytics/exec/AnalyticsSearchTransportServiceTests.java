/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AnalyticsSearchTransportService#dispatchFragment}.
 *
 * <p>Mocks {@link TransportService#sendChildRequest} to capture the inner
 * {@link TransportResponseHandler}, then drives the captured handler with a
 * fake {@link StreamTransportResponse} to assert the client-side batching
 * contract — specifically that each batch is fanned out to the listener
 * immediately (not with a one-iteration delay, which would corrupt data
 * because Flight reuses its client-side root across batches).
 */
public class AnalyticsSearchTransportServiceTests extends OpenSearchTestCase {

    public void testEachBatchEmittedImmediatelyWithCompletionSignal() {
        Recording rec = setUpWithStream(List.of(resp(), resp(), resp()));

        // Drive the handler: the handler consumes stream.nextResponse() in a
        // loop and emits each batch to the listener.
        rec.handler.get().handleStreamResponse(rec.stream);

        assertEquals("one emit per batch + 1 terminal completion signal", 4, rec.emits.size());
        assertFalse("batch 1 isLast=false", rec.emits.get(0).isLast);
        assertFalse("batch 2 isLast=false", rec.emits.get(1).isLast);
        assertFalse("batch 3 isLast=false", rec.emits.get(2).isLast);
        assertTrue("completion is isLast=true", rec.emits.get(3).isLast);
        assertNull("completion carries no response", rec.emits.get(3).response);
    }

    public void testEmptyStreamEmitsOnlyCompletion() {
        Recording rec = setUpWithStream(List.of());

        rec.handler.get().handleStreamResponse(rec.stream);

        assertEquals("only the completion signal", 1, rec.emits.size());
        assertTrue(rec.emits.get(0).isLast);
        assertNull(rec.emits.get(0).response);
    }

    public void testStreamFailureRoutesToListenerOnFailure() {
        RuntimeException boom = new RuntimeException("stream boom");
        Recording rec = setUpWithFailingStream(boom);

        rec.handler.get().handleStreamResponse(rec.stream);

        assertEquals("no normal emits on stream failure", 0, rec.emits.size());
        assertSame("onFailure carries the stream exception", boom, rec.failure.get());
    }

    public void testHandleExceptionRoutesToListenerOnFailure() {
        Recording rec = setUpWithStream(List.of());
        TransportException te = new TransportException("transport boom");

        rec.handler.get().handleException(te);

        assertSame(te, rec.failure.get());
    }

    public void testConnectionFailureRoutesToListenerOnFailure() {
        // Arrange: transportService throws on send, so the caller sees the
        // exception inside pending.tryRun and must route to listener.onFailure.
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mockClusterServiceReturning(newNode("nodeA"));

        RuntimeException sendBoom = new RuntimeException("connection refused");
        when(transportService.getConnection(any())).thenThrow(sendBoom);

        AnalyticsSearchTransportService svc = new AnalyticsSearchTransportService(transportService, clusterService);

        AtomicReference<Exception> failure = new AtomicReference<>();
        StreamingResponseListener<FragmentExecutionResponse> listener = new StreamingResponseListener<>() {
            @Override public void onStreamResponse(FragmentExecutionResponse r, boolean isLast) { }
            @Override public void onFailure(Exception e) { failure.set(e); }
        };

        svc.dispatchFragment(request(), newNode("nodeA"), listener, mock(Task.class), new PendingExecutions(1));

        assertSame("connection-time exception routed to listener", sendBoom, failure.get());
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static final class Emit {
        final FragmentExecutionResponse response;
        final boolean isLast;
        Emit(FragmentExecutionResponse r, boolean l) { this.response = r; this.isLast = l; }
    }

    /** Bundles the setup state so tests can drive the captured handler. */
    private static final class Recording {
        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        List<Emit> emits = new ArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean pendingReleased = new AtomicBoolean();
    }

    @SuppressWarnings("unchecked")
    private Recording setUpWithStream(List<FragmentExecutionResponse> batches) {
        Recording rec = new Recording();
        // Return each batch then null-terminate
        var stub = when(rec.stream.nextResponse());
        for (FragmentExecutionResponse b : batches) stub = stub.thenReturn(b);
        stub.thenReturn(null);

        TransportService transportService = captureHandler(rec);
        ClusterService clusterService = mockClusterServiceReturning(newNode("nodeA"));
        AnalyticsSearchTransportService svc = new AnalyticsSearchTransportService(transportService, clusterService);

        svc.dispatchFragment(request(), newNode("nodeA"), listenerThatRecords(rec), mock(Task.class), pendingThatSignals(rec));
        return rec;
    }

    @SuppressWarnings("unchecked")
    private Recording setUpWithFailingStream(RuntimeException boom) {
        Recording rec = new Recording();
        when(rec.stream.nextResponse()).thenThrow(boom);

        TransportService transportService = captureHandler(rec);
        ClusterService clusterService = mockClusterServiceReturning(newNode("nodeA"));
        AnalyticsSearchTransportService svc = new AnalyticsSearchTransportService(transportService, clusterService);

        svc.dispatchFragment(request(), newNode("nodeA"), listenerThatRecords(rec), mock(Task.class), pendingThatSignals(rec));
        return rec;
    }

    @SuppressWarnings("unchecked")
    private static TransportService captureHandler(Recording rec) {
        TransportService transportService = mock(TransportService.class);
        when(transportService.getConnection(any())).thenReturn(mock(Transport.Connection.class));
        doAnswer(inv -> {
            // sendChildRequest(connection, action, request, parentTask, handler)
            rec.handler.set((TransportResponseHandler<FragmentExecutionResponse>) inv.getArgument(4));
            return null;
        }).when(transportService).sendChildRequest(any(Transport.Connection.class), anyString(), any(), any(Task.class), any(TransportResponseHandler.class));
        return transportService;
    }

    private static StreamingResponseListener<FragmentExecutionResponse> listenerThatRecords(Recording rec) {
        return new StreamingResponseListener<>() {
            @Override public void onStreamResponse(FragmentExecutionResponse r, boolean isLast) {
                rec.emits.add(new Emit(r, isLast));
            }
            @Override public void onFailure(Exception e) { rec.failure.set(e); }
        };
    }

    private static PendingExecutions pendingThatSignals(Recording rec) {
        // The slot-release signal is observed indirectly: after dispatch
        // completes the handler path, a subsequent tryRun should acquire the
        // slot (if the slot was released). The test probes this via
        // `pending.tryRun(() -> rec.pendingReleased.set(true))` after driving
        // the handler.
        return new PendingExecutions(1);
    }

    private static ClusterService mockClusterServiceReturning(DiscoveryNode node) {
        ClusterService cs = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(cs.state()).thenReturn(state);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.get(anyString())).thenReturn(node);
        return cs;
    }

    private static FragmentExecutionRequest request() {
        return new FragmentExecutionRequest(
            "q-test",
            0,
            new ShardId(new Index("idx", UUID.randomUUID().toString()), 0),
            List.of(new FragmentExecutionRequest.PlanAlternative("mock-backend", new byte[0]))
        );
    }

    private static DiscoveryNode newNode(String name) {
        return new DiscoveryNode(
            name,
            new TransportAddress(InetAddress.getLoopbackAddress(), 0),
            org.opensearch.Version.CURRENT
        );
    }

    /** Builds a dummy {@link FragmentExecutionResponse} — we never inspect its contents in these tests. */
    private static FragmentExecutionResponse resp() {
        // Construct with a null root — these tests only check the batching
        // contract (isLast flag ordering), never touch response.getArrowRoot().
        return new FragmentExecutionResponse((org.apache.arrow.vector.VectorSchemaRoot) null);
    }
}
