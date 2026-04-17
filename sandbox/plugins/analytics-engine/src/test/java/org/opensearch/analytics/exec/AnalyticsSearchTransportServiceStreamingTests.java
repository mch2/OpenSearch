/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
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
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
 * Tests for streaming transport dispatch in {@link AnalyticsSearchTransportService}.
 *
 * <p>These tests use {@link AnalyticsSearchTransportService#dispatchFragment} and verify
 * the look-ahead pattern that detects {@code isLast}.
 *
 * Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.5
 */
@SuppressWarnings("unchecked")
public class AnalyticsSearchTransportServiceStreamingTests extends OpenSearchTestCase {

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
        VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(root.getRowCount()).thenReturn(1);
        return new FragmentExecutionResponse(root);
    }

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

    private TransportResponseHandler<FragmentExecutionResponse> captureHandler(
        StreamTransportService transportService,
        AnalyticsSearchTransportService dispatcher,
        StreamingResponseListener<FragmentExecutionResponse> listener
    ) {
        ArgumentCaptor<TransportResponseHandler<FragmentExecutionResponse>> captor = ArgumentCaptor.forClass(
            TransportResponseHandler.class
        );

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        dispatcher.dispatchFragment(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(transportService).sendChildRequest(
            any(Transport.Connection.class),
            eq(FragmentExecutionAction.NAME),
            any(TransportRequest.class),
            any(Task.class),
            captor.capture()
        );

        return captor.getValue();
    }

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

    public void testSingleBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse batch = dummyBatch("batch-1");
        StreamTransportResponse<FragmentExecutionResponse> stream = mockStream(batch);

        handler.handleStreamResponse(stream);

        verify(listener, times(1)).onStreamResponse(batch, true);
        verify(listener, never()).onStreamResponse(any(), eq(false));
        verify(listener, never()).onFailure(any());
    }

    public void testMultiBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse b1 = dummyBatch("b1");
        FragmentExecutionResponse b2 = dummyBatch("b2");
        FragmentExecutionResponse b3 = dummyBatch("b3");
        StreamTransportResponse<FragmentExecutionResponse> stream = mockStream(b1, b2, b3);

        handler.handleStreamResponse(stream);

        verify(listener).onStreamResponse(b1, false);
        verify(listener).onStreamResponse(b2, false);
        verify(listener).onStreamResponse(b3, true);
        verify(listener, never()).onFailure(any());
    }

    public void testStreamExceptionMidFlight() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse b1 = dummyBatch("b1");
        FragmentExecutionResponse b2 = dummyBatch("b2");
        RuntimeException midFlightError = new RuntimeException("stream broke");

        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenReturn(b1).thenReturn(b2).thenThrow(midFlightError);

        handler.handleStreamResponse(stream);

        verify(listener).onStreamResponse(b1, false);
        verify(listener, never()).onStreamResponse(eq(b2), eq(true));
        verify(listener, never()).onStreamResponse(eq(b2), eq(false));
        verify(listener, times(1)).onFailure(midFlightError);
    }

    public void testStreamClosedInFinally() throws Exception {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        RuntimeException error = new RuntimeException("stream error");
        StreamTransportResponse<FragmentExecutionResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenThrow(error);

        handler.handleStreamResponse(stream);

        verify(stream, times(1)).close();
    }

    public void testNonStreamingFallback() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<FragmentExecutionResponse> handler = captureHandler(transportService, dispatcher, listener);

        FragmentExecutionResponse response = dummyBatch("single-response");

        handler.handleResponse(response);

        verify(listener, times(1)).onStreamResponse(response, true);
        verify(listener, never()).onFailure(any());
    }

    public void testPermitReleasedOnSuccess() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler1Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler1Ref.set(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener<FragmentExecutionResponse> listener1 = mock(StreamingResponseListener.class);
        PendingExecutions pending = new PendingExecutions(1);
        dispatcher.dispatchFragment(dummyRequest(), node, listener1, parentTask, pending);
        assertNotNull("handler1 must be captured", handler1Ref.get());

        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler2Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler2Ref.set(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener<FragmentExecutionResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchFragment(dummyRequest(), node, listener2, parentTask, pending);
        assertNull("handler2 must be queued (not dispatched yet)", handler2Ref.get());

        handler1Ref.get().handleResponse(dummyBatch("done"));

        assertNotNull("handler2 must be dispatched after permit release", handler2Ref.get());
    }

    public void testPermitReleasedOnFailure() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler1Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler1Ref.set(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener<FragmentExecutionResponse> listener1 = mock(StreamingResponseListener.class);
        PendingExecutions pendingF = new PendingExecutions(1);
        dispatcher.dispatchFragment(dummyRequest(), node, listener1, parentTask, pendingF);
        assertNotNull("handler1 must be captured", handler1Ref.get());

        AtomicReference<TransportResponseHandler<FragmentExecutionResponse>> handler2Ref = new AtomicReference<>();
        doAnswer(invocation -> {
            handler2Ref.set(invocation.getArgument(4));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportResponseHandler.class)
            );

        StreamingResponseListener<FragmentExecutionResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchFragment(dummyRequest(), node, listener2, parentTask, pendingF);
        assertNull("handler2 must be queued", handler2Ref.get());

        handler1Ref.get().handleException(new TransportException("connection lost"));

        assertNotNull("handler2 must be dispatched after failure releases permit", handler2Ref.get());
    }

    public void testConnectionLookupUsed() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        Transport.Connection mockConnection = mock(Transport.Connection.class);

        ClusterService cs = mockClusterService();
        DiscoveryNode resolvedNode = cs.state().nodes().get("node-1");
        when(transportService.getConnection(resolvedNode)).thenReturn(mockConnection);

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, cs);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);

        doAnswer(invocation -> {
            TransportResponseHandler<FragmentExecutionResponse> handler = invocation.getArgument(4);
            handler.handleResponse(dummyBatch("done"));
            return null;
        }).when(transportService)
            .sendChildRequest(
                any(Transport.Connection.class),
                anyString(),
                any(TransportRequest.class),
                any(Task.class),
                any(TransportResponseHandler.class)
            );

        dispatcher.dispatchFragment(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(transportService).getConnection(resolvedNode);

        verify(transportService).sendChildRequest(
            eq(mockConnection),
            eq(FragmentExecutionAction.NAME),
            any(TransportRequest.class),
            eq(parentTask),
            any(TransportResponseHandler.class)
        );
    }

    public void testConnectionLookupFailureRoutesToOnFailure() {
        StreamTransportService transportService = mock(StreamTransportService.class);
        RuntimeException lookupFailure = new RuntimeException("node not found");

        ClusterService cs = mockClusterService();
        when(transportService.getConnection(any(DiscoveryNode.class))).thenThrow(lookupFailure);

        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, cs);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        StreamingResponseListener<FragmentExecutionResponse> listener = mock(StreamingResponseListener.class);
        dispatcher.dispatchFragment(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(listener, times(1)).onFailure(lookupFailure);
        verify(listener, never()).onStreamResponse(any(), eq(true));
        verify(listener, never()).onStreamResponse(any(), eq(false));

        StreamingResponseListener<FragmentExecutionResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchFragment(dummyRequest(), node, listener2, parentTask, new PendingExecutions(5));

        verify(listener2, times(1)).onFailure(lookupFailure);
    }
}
