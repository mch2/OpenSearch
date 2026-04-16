/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for streaming transport dispatch in {@link ShardTransportDispatcher}.
 *
 * <p>These tests capture the {@link TransportResponseHandler} passed to
 * {@link StreamTransportService#sendChildRequest} and invoke
 * {@code handleStreamResponse(StreamTransportResponse)} on it to verify
 * the look-ahead pattern that detects {@code isLast}.
 *
 * Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.5
 */
@SuppressWarnings("unchecked")
public class ShardTransportDispatcherStreamingTests extends OpenSearchTestCase {

    // ─── Helpers ────────────────────────────────────────────────────────

    private FragmentExecutionRequest dummyRequest() {
        return new FragmentExecutionRequest(
            "test-query",
            0,
            new org.opensearch.core.index.shard.ShardId(new org.opensearch.core.index.Index("test_index", "_na_"), 0),
            List.of()
        );
    }

    private FragmentExecutionResponse dummyBatch(String label) {
        return new FragmentExecutionResponse(List.of("field"), Collections.singletonList(new Object[] { label }));
    }

    /**
     * Creates a mock {@link StreamTransportService} and a connection lookup
     * that returns a mock {@link Transport.Connection}.
     */
    private StreamTransportService mockStreamTransportService() {
        StreamTransportService sts = mock(StreamTransportService.class);
        when(sts.getConnection(any(DiscoveryNode.class))).thenReturn(mock(Transport.Connection.class));
        return sts;
    }

    private ClusterService mockClusterService() {
        ClusterService cs = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.get(any())).thenReturn(mock(DiscoveryNode.class));
        when(state.nodes()).thenReturn(nodes);
        when(cs.state()).thenReturn(state);
        return cs;
    }

    /**
     * Dispatches a request through the dispatcher and captures the
     * {@link TransportResponseHandler} passed to
     * {@code sendChildRequest(Connection, ...)} overload.
     */
    private TransportResponseHandler<FragmentExecutionResponse> captureHandler(
        StreamTransportService transportService,
        ShardTransportDispatcher dispatcher,
        StreamingResponseListener listener
    ) {
        ArgumentCaptor<TransportResponseHandler<FragmentExecutionResponse>> captor = ArgumentCaptor.forClass(
            TransportResponseHandler.class
        );

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        dispatcher.dispatch(dummyRequest(), node, listener, parentTask, pendingPerNode);

        verify(transportService).sendChildRequest(
            any(Transport.Connection.class),
            eq(AnalyticsShardAction.NAME),
            any(TransportRequest.class),
            any(Task.class),
            any(TransportRequestOptions.class),
            captor.capture()
        );

        return captor.getValue();
    }

    /**
     * Creates a mock {@link StreamTransportResponse} that returns the given
     * batches in sequence, then null.
     */
    private StreamTransportResponse<FragmentExecutionResponse> mockStream(FragmentExecutionResponse... batches) {
        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        if (batches.length == 0) {
            when(stream.nextResponse()).thenReturn(null);
        } else if (batches.length == 1) {
            when(stream.nextResponse()).thenReturn(batches[0]).thenReturn(null);
        } else {
            FragmentExecutionResponse[] rest = new FragmentExecutionResponse[batches.length];
            System.arraycopy(batches, 1, rest, 0, batches.length - 1);
            rest[batches.length - 1] = null;
            when(stream.nextResponse()).thenReturn(batches[0], rest);
        }
        return stream;
    }

    // ─── 9.1 Single batch stream ────────────────────────────────────────

    /**
     * 9.1: 1 batch + null → {@code onStreamResponse(batch, true)} once.
     * The look-ahead pattern buffers one response; when {@code nextResponse()}
     * returns null, the buffered response is emitted as {@code isLast=true}.
     *
     * Validates: Requirements 3.3
     */
    public void testSingleBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 5);
        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse batch = dummyBatch("batch-1");
        StreamTransportResponse<FragmentExecutionResponse> stream = mockStream(batch);

        handler.handleStreamResponse(stream);

        verify(listener, times(1)).onStreamResponse(batch, true);
        verify(listener, never()).onStreamResponse(any(), eq(false));
        verify(listener, never()).onFailure(any());
    }

    // ─── 9.2 Multi-batch stream ─────────────────────────────────────────

    /**
     * 9.2: 3 batches + null → {@code onStreamResponse(b1, false)},
     * {@code onStreamResponse(b2, false)}, {@code onStreamResponse(b3, true)}.
     *
     * Validates: Requirements 3.3
     */
    public void testMultiBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 5);
        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse b1 = dummyBatch("b1");
        FragmentExecutionResponse b2 = dummyBatch("b2");
        FragmentExecutionResponse b3 = dummyBatch("b3");
        StreamTransportResponse<FragmentExecutionResponse> stream = mockStream(b1, b2, b3);

        handler.handleStreamResponse(stream);

        // b1 and b2 are intermediate (isLast=false), b3 is final (isLast=true)
        verify(listener).onStreamResponse(b1, false);
        verify(listener).onStreamResponse(b2, false);
        verify(listener).onStreamResponse(b3, true);
        verify(listener, never()).onFailure(any());
    }

    // ─── 9.3 Stream exception mid-flight ────────────────────────────────

    /**
     * 9.3: Stream yields b1, then b2, then throws on the 3rd call →
     * {@code onStreamResponse(b1, false)}, {@code onFailure(e)}.
     *
     * <p>Look-ahead trace:
     * <ol>
     *   <li>{@code nextResponse()} → b1, last=null → last=b1</li>
     *   <li>{@code nextResponse()} → b2, last=b1 → emit {@code onStreamResponse(b1, false)}, last=b2</li>
     *   <li>{@code nextResponse()} → throws → catch → {@code onFailure(e)}</li>
     * </ol>
     * b2 is buffered but never emitted because the exception fires before the loop continues.
     *
     * Validates: Requirements 3.3
     */
    public void testStreamExceptionMidFlight() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 5);
        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse b1 = dummyBatch("b1");
        FragmentExecutionResponse b2 = dummyBatch("b2");
        RuntimeException midFlightError = new RuntimeException("stream broke");

        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenReturn(b1).thenReturn(b2).thenThrow(midFlightError);

        handler.handleStreamResponse(stream);

        // b1 emitted as intermediate when b2 was read (look-ahead)
        verify(listener).onStreamResponse(b1, false);
        // b2 was buffered as 'last' but never emitted — exception caught first
        verify(listener, never()).onStreamResponse(eq(b2), eq(true));
        verify(listener, never()).onStreamResponse(eq(b2), eq(false));
        // Exception routed to onFailure
        verify(listener, times(1)).onFailure(midFlightError);
    }

    // ─── 9.4 Stream closed in finally ───────────────────────────────────

    /**
     * 9.4: {@code stream.close()} is called even when an exception occurs
     * during stream processing.
     *
     * Validates: Requirements 3.3, 3.5
     */
    public void testStreamClosedInFinally() throws Exception {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 5);
        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        RuntimeException error = new RuntimeException("stream error");
        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenThrow(error);

        handler.handleStreamResponse(stream);

        // stream.close() must be called in the finally block regardless of exception
        verify(stream, times(1)).close();
    }

    // ─── 9.5 Non-streaming fallback ─────────────────────────────────────

    /**
     * 9.5: {@code handleResponse(response)} path → {@code onStreamResponse(resp, true)}.
     * This is the non-streaming fallback used by {@code MockTransportService} in tests.
     *
     * Validates: Requirements 3.4
     */
    public void testNonStreamingFallback() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 5);
        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse response = dummyBatch("single-response");

        handler.handleResponse(response);

        // Non-streaming path wraps as a single final response
        verify(listener, times(1)).onStreamResponse(response, true);
        verify(listener, never()).onFailure(any());
    }

    // ─── 9.6 Permit released on success ─────────────────────────────────

    /**
     * 9.6: After the final response, the {@code PendingExecutions} permit is
     * freed (via {@code finishAndRunNext}).
     *
     * <p>We verify this by dispatching with permits=1, completing the first
     * request, then verifying a queued second request runs.
     *
     * Validates: Requirements 3.5
     */
    public void testPermitReleasedOnSuccess() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 1);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        // Capture handlers via doAnswer on the Connection-based sendChildRequest
        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler1Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler1Ref.set(invocation.getArgument(5));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener listener1 = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener1, parentTask, pendingPerNode);
        assertNotNull("handler1 must be captured", handler1Ref.get());

        // Second dispatch — should be queued (permits=1, first still in-flight)
        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler2Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler2Ref.set(invocation.getArgument(5));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener2, parentTask, pendingPerNode);
        assertNull("handler2 must be queued (not dispatched yet)", handler2Ref.get());

        // Complete first request via handleResponse (non-streaming fallback)
        handler1Ref.get().handleResponse(dummyBatch("done"));

        // Now the queued second request should have been dispatched
        assertNotNull("handler2 must be dispatched after permit release", handler2Ref.get());
    }

    // ─── 9.7 Permit released on failure ─────────────────────────────────

    /**
     * 9.7: After a failure, the {@code PendingExecutions} permit is freed.
     *
     * Validates: Requirements 3.5
     */
    public void testPermitReleasedOnFailure() {
        StreamTransportService transportService = mockStreamTransportService();
        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, mockClusterService(), 1);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        // First dispatch — captures handler
        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler1Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler1Ref.set(invocation.getArgument(5));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener listener1 = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener1, parentTask, pendingPerNode);
        assertNotNull("handler1 must be captured", handler1Ref.get());

        // Second dispatch — should be queued
        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler2Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler2Ref.set(invocation.getArgument(5));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener2, parentTask, pendingPerNode);
        assertNull("handler2 must be queued", handler2Ref.get());

        // Fail first request via handleException
        handler1Ref.get().handleException(new TransportException("connection lost"));

        // Permit released → queued second request dispatched
        assertNotNull("handler2 must be dispatched after failure releases permit", handler2Ref.get());
    }

    // ─── 9.8 Connection lookup used ─────────────────────────────────────

    /**
     * 9.8: {@code getConnection(null, nodeId)} is invoked before dispatch.
     * The resolved {@code Transport.Connection} is passed to {@code sendChildRequest}.
     *
     * Validates: Requirements 4.1, 4.2
     */
    public void testConnectionLookupUsed() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        Transport.Connection mockConnection = mock(Transport.Connection.class);

        // Mock ClusterService to resolve node-1 → a DiscoveryNode, then transportService.getConnection → mockConnection
        ClusterService cs = mockClusterService();
        DiscoveryNode resolvedNode = cs.state().nodes().get("node-1");
        when(transportService.getConnection(resolvedNode)).thenReturn(mockConnection);

        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, cs, 5);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        StreamingResponseListener listener = mock(StreamingResponseListener.class);

        // Mock the Connection-based sendChildRequest to respond immediately
        doAnswer(invocation -> {
            TransportResponseHandler<FragmentExecutionResponse> handler = invocation.getArgument(5);
            handler.handleResponse(dummyBatch("done"));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportRequestOptions.class),
                any(TransportResponseHandler.class)
            );

        dispatcher.dispatch(dummyRequest(), node, listener, parentTask, pendingPerNode);

        // Verify transportService.getConnection was called to resolve the connection
        verify(transportService).getConnection(resolvedNode);

        // Verify the Connection-based sendChildRequest was called with our mock connection
        verify(transportService).sendChildRequest(
            eq(mockConnection),
            eq(AnalyticsShardAction.NAME),
            any(TransportRequest.class),
            eq(parentTask),
            any(TransportRequestOptions.class),
            any(TransportResponseHandler.class)
        );
    }

    // ─── 9.9 Connection lookup failure routes to onFailure ──────────────

    /**
     * 9.9: When connection lookup throws, the exception is routed to
     * {@code listener.onFailure} and the permit is released.
     *
     * Validates: Requirements 4.5
     */
    public void testConnectionLookupFailureRoutesToOnFailure() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        RuntimeException lookupFailure = new RuntimeException("node not found");

        // Mock ClusterService where getConnection throws
        ClusterService cs = mockClusterService();
        when(transportService.getConnection(any(DiscoveryNode.class))).thenThrow(lookupFailure);

        ShardTransportDispatcher dispatcher = new ShardTransportDispatcher(transportService, cs, 1);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);
        Map<String, ShardTransportDispatcher.PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

        StreamingResponseListener listener = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener, parentTask, pendingPerNode);

        // Connection lookup failure should route to listener.onFailure
        verify(listener, times(1)).onFailure(lookupFailure);
        verify(listener, never()).onStreamResponse(any(), eq(true));
        verify(listener, never()).onStreamResponse(any(), eq(false));

        // Permit must be released — verify by dispatching a second request that runs immediately
        // Fix the lookup for the second dispatch
        Transport.Connection mockConnection = mock(Transport.Connection.class);

        // Create a new dispatcher with working lookup for the second dispatch verification
        BiFunction<String, String, Transport.Connection> workingLookup = (alias, nodeId) -> mockConnection;
        // Instead, we verify permit release by dispatching again on the same dispatcher
        // The lookup still throws, but the permit should be available
        StreamingResponseListener listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatch(dummyRequest(), node, listener2, parentTask, pendingPerNode);

        // Second request should dispatch immediately (permit was released after first failure)
        // It will also fail with the same lookup error, but the point is it ran (wasn't queued)
        verify(listener2, times(1)).onFailure(lookupFailure);
    }
}
