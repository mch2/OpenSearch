/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.distributed;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Resolves each targeted node's {@code datafusion-distributed} Worker gRPC port by calling
 * {@link AnalyticsSearchBackendPlugin#getWorkerPort()} on the local node's {@code datafusion}
 * backend (which reads {@code DataFusionService.getWorkerPort()} — the port bound by
 * {@code DataFusionWorkerAuxTransport}). Backs {@link GetWorkerPortAction}.
 *
 * @opensearch.internal
 */
public final class TransportGetWorkerPortAction extends TransportNodesAction<
    GetWorkerPortAction.Request,
    GetWorkerPortAction.Response,
    TransportGetWorkerPortAction.NodeRequest,
    GetWorkerPortAction.NodeResponse> {

    /** The backend whose Worker port we report; matches {@code DefaultPlanExecutor.DATAFUSION_BACKEND}. */
    private static final String DATAFUSION_BACKEND = "datafusion";

    private final CapabilityRegistry capabilityRegistry;

    @Inject
    public TransportGetWorkerPortAction(
        ThreadPool threadPool,
        org.opensearch.cluster.service.ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CapabilityRegistry capabilityRegistry
    ) {
        super(
            GetWorkerPortAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetWorkerPortAction.Request::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            GetWorkerPortAction.NodeResponse.class
        );
        this.capabilityRegistry = capabilityRegistry;
    }

    @Override
    protected GetWorkerPortAction.Response newResponse(
        GetWorkerPortAction.Request request,
        List<GetWorkerPortAction.NodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new GetWorkerPortAction.Response(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(GetWorkerPortAction.Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected GetWorkerPortAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetWorkerPortAction.NodeResponse(in);
    }

    @Override
    protected GetWorkerPortAction.NodeResponse nodeOperation(NodeRequest nodeRequest) {
        int port = -1;
        AnalyticsSearchBackendPlugin backend = capabilityRegistry.getBackend(DATAFUSION_BACKEND);
        if (backend != null) {
            port = backend.getWorkerPort();
        }
        return new GetWorkerPortAction.NodeResponse(transportService.getLocalNode(), port);
    }

    /** Node-level request wrapper (carries no extra state). */
    public static final class NodeRequest extends TransportRequest {
        @SuppressWarnings("unused")
        private final GetWorkerPortAction.Request request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new GetWorkerPortAction.Request(in);
        }

        public NodeRequest(GetWorkerPortAction.Request request) {
            this.request = Objects.requireNonNull(request);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
