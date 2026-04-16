/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.AnalyticsScanAction;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
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
 * <p>These tests use {@link AnalyticsSearchTransportService#dispatchScan} and verify
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

    private ScanResponse dummyBatch(String label) {
        return new ScanResponse(List.of("field"), Collections.singletonList(new Object[] { label }));
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

    private TransportResponseHandler<ScanResponse> captureHandler(
        StreamTransportService transportService,
        AnalyticsSearchTransportService dispatcher,
        StreamingResponseListener<ScanResponse> listener
    ) {
        ArgumentCaptor<TransportResponseHandler<ScanResponse>> captor = ArgumentCaptor.forClass(TransportResponseHandler.class);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-1");
        Task parentTask = mock(Task.class);

        dispatcher.dispatchScan(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(transportService).sendChildRequest(
            any(Transport.Connection.class),
            eq(AnalyticsScanAction.NAME),
            any(TransportRequest.class),
            any(Task.class),
            any(TransportRequestOptions.class),
            captor.capture()
        );

        return captor.getValue();
    }

    private StreamTransportResponse<ScanResponse> mockStream(ScanResponse... batches) {
        StreamTransportResponse<ScanResponse> stream = mock(StreamTransportResponse.class);
        if (batches.length == 0) {
            when(stream.nextResponse()).thenReturn(null);
        } else if (batches.length == 1) {
            when(stream.nextResponse()).thenReturn(batches[0]).thenReturn(null);
        } else {
            ScanResponse[] rest = new ScanResponse[batches.length];
            System.arraycopy(batches, 1, rest, 0, batches.length - 1);
            rest[batches.length - 1] = null;
            when(stream.nextResponse()).thenReturn(batches[0], rest);
        }
        return stream;
    }

    public void testSingleBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<ScanResponse> handler = captureHandler(transportService, dispatcher, listener);

        ScanResponse batch = dummyBatch("batch-1");
        StreamTransportResponse<ScanResponse> stream = mockStream(batch);

        handler.handleStreamResponse(stream);

        verify(listener, times(1)).onStreamResponse(batch, true);
        verify(listener, never()).onStreamResponse(any(), eq(false));
        verify(listener, never()).onFailure(any());
    }

    public void testMultiBatchStream() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<ScanResponse> handler = captureHandler(transportService, dispatcher, listener);

        ScanResponse b1 = dummyBatch("b1");
        ScanResponse b2 = dummyBatch("b2");
        ScanResponse b3 = dummyBatch("b3");
        StreamTransportResponse<ScanResponse> stream = mockStream(b1, b2, b3);

        handler.handleStreamResponse(stream);

        verify(listener).onStreamResponse(b1, false);
        verify(listener).onStreamResponse(b2, false);
        verify(listener).onStreamResponse(b3, true);
        verify(listener, never()).onFailure(any());
    }

    public void testStreamExceptionMidFlight() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<ScanResponse> handler = captureHandler(transportService, dispatcher, listener);

        ScanResponse b1 = dummyBatch("b1");
        ScanResponse b2 = dummyBatch("b2");
        RuntimeException midFlightError = new RuntimeException("stream broke");

        StreamTransportResponse<ScanResponse> stream = mock(StreamTransportResponse.class);
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
        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<ScanResponse> handler = captureHandler(transportService, dispatcher, listener);

        RuntimeException error = new RuntimeException("stream error");
        StreamTransportResponse<ScanResponse> stream = mock(StreamTransportResponse.class);
        when(stream.nextResponse()).thenThrow(error);

        handler.handleStreamResponse(stream);

        verify(stream, times(1)).close();
    }

    public void testNonStreamingFallback() {
        StreamTransportService transportService = mockStreamTransportService();
        AnalyticsSearchTransportService dispatcher = new AnalyticsSearchTransportService(transportService, mockClusterService());
        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        TransportResponseHandler<ScanResponse> handler = captureHandler(transportService, dispatcher, listener);

        ScanResponse response = dummyBatch("single-response");

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

        AtomicReference<TransportResponseHandler<ScanResponse>> handler1Ref = new AtomicReference<>();
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

        StreamingResponseListener<ScanResponse> listener1 = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener1, parentTask, new PendingExecutions(5));
        assertNotNull("handler1 must be captured", handler1Ref.get());

        AtomicReference<TransportResponseHandler<ScanResponse>> handler2Ref = new AtomicReference<>();
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

        StreamingResponseListener<ScanResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener2, parentTask, new PendingExecutions(5));
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

        AtomicReference<TransportResponseHandler<ScanResponse>> handler1Ref = new AtomicReference<>();
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

        StreamingResponseListener<ScanResponse> listener1 = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener1, parentTask, new PendingExecutions(5));
        assertNotNull("handler1 must be captured", handler1Ref.get());

        AtomicReference<TransportResponseHandler<ScanResponse>> handler2Ref = new AtomicReference<>();
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

        StreamingResponseListener<ScanResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener2, parentTask, new PendingExecutions(5));
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

        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);

        doAnswer(invocation -> {
            TransportResponseHandler<ScanResponse> handler = invocation.getArgument(5);
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

        dispatcher.dispatchScan(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(transportService).getConnection(resolvedNode);

        verify(transportService).sendChildRequest(
            eq(mockConnection),
            eq(AnalyticsScanAction.NAME),
            any(TransportRequest.class),
            eq(parentTask),
            any(TransportRequestOptions.class),
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

        StreamingResponseListener<ScanResponse> listener = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener, parentTask, new PendingExecutions(5));

        verify(listener, times(1)).onFailure(lookupFailure);
        verify(listener, never()).onStreamResponse(any(), eq(true));
        verify(listener, never()).onStreamResponse(any(), eq(false));

        StreamingResponseListener<ScanResponse> listener2 = mock(StreamingResponseListener.class);
        dispatcher.dispatchScan(dummyRequest(), node, listener2, parentTask, new PendingExecutions(5));

        verify(listener2, times(1)).onFailure(lookupFailure);
    }
}
