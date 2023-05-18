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
import org.opensearch.indices.replication.common.SegmentReplicationTransportRequest;

import java.io.IOException;
import java.util.List;

public class SegmentReplicationFileInfoRequest extends SegmentReplicationTransportRequest {

    List<StoreFileMetadata> files;

    protected SegmentReplicationFileInfoRequest(long replicationId, String targetAllocationId, DiscoveryNode targetNode, List<StoreFileMetadata> files) {
        super(replicationId, targetAllocationId, targetNode);
        this.files = files;
    }

    protected SegmentReplicationFileInfoRequest(StreamInput in) throws IOException {
        super(in);
        files = in.readList(StoreFileMetadata::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(files);
    }

    public List<StoreFileMetadata> getFiles() {
        return files;
    }
}
