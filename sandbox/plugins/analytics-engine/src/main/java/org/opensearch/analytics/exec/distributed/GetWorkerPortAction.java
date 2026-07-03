/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.distributed;

import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Discovery action that asks the targeted nodes for the bound TCP port of their
 * {@code datafusion-distributed} Worker gRPC server (the rust↔rust data-plane endpoint owned by
 * {@code DataFusionWorkerAuxTransport}). The coordinator dials each node directly at
 * {@code http://<node-host>:<port>}; replaces the old {@code node.attr.datafusion_grpc_port}
 * advertisement, which could not carry an ephemeral, post-bind port (and broke 2-nodes-per-host).
 *
 * <p>A node that is not running the Worker (older native lib, or the {@code datafusion-worker} aux
 * transport not enabled) reports {@code -1}; the coordinator treats that as "cannot route here".
 *
 * @opensearch.internal
 */
public final class GetWorkerPortAction extends ActionType<GetWorkerPortAction.Response> {

    public static final GetWorkerPortAction INSTANCE = new GetWorkerPortAction();
    public static final String NAME = "cluster:admin/analytics/datafusion/worker_port";

    private GetWorkerPortAction() {
        super(NAME, Response::new);
    }

    /** Nodes-request; targets specific node ids (the nodes hosting the query's shards). */
    public static final class Request extends BaseNodesRequest<Request> {
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(String... nodeIds) {
            super(nodeIds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    /** Aggregated per-node worker ports. */
    public static final class Response extends BaseNodesResponse<NodeResponse> {
        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /** One node's bound Worker gRPC port, or {@code -1} when not running. */
    public static final class NodeResponse extends BaseNodeResponse {
        private final int workerPort;

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.workerPort = in.readInt();
        }

        public NodeResponse(DiscoveryNode node, int workerPort) {
            super(node);
            this.workerPort = workerPort;
        }

        public int getWorkerPort() {
            return workerPort;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(workerPort);
        }
    }
}
