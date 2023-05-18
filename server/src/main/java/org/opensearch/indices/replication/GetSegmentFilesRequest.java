/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.SegmentReplicationTransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Request object for fetching a list of segment files metadata from a {@link SegmentReplicationSource}.
 * This object is created by the target node and sent to the source node.
 *
 * @opensearch.internal
 */
public class GetSegmentFilesRequest extends SegmentReplicationTransportRequest {

    private final Map<String, StoreFileMetadata> metadataMap;
    private final ReplicationCheckpoint checkpoint;

    public GetSegmentFilesRequest(StreamInput in) throws IOException {
        super(in);
        this.metadataMap = in.readMap(StreamInput::readString, StoreFileMetadata::new);
        this.checkpoint = new ReplicationCheckpoint(in);
    }

    public GetSegmentFilesRequest(
        long replicationId,
        String targetAllocationId,
        DiscoveryNode targetNode,
        Map<String, StoreFileMetadata> metadataMap,
        ReplicationCheckpoint checkpoint
    ) {
        super(replicationId, targetAllocationId, targetNode);
        this.checkpoint = checkpoint;
        this.metadataMap = metadataMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(metadataMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        checkpoint.writeTo(out);
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public Map<String, StoreFileMetadata> getMetadataMap() {
        return metadataMap;
    }
}
