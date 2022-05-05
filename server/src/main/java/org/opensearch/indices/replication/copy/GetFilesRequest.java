/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.apache.lucene.replicator.nrt.FileMetaData;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.List;

public class GetFilesRequest extends SegmentReplicationTransportRequest {

    private final String fileName;
    private final ShardId shardId;
//    private final ReplicationCheckpoint checkpoint;

    public GetFilesRequest(
        long replicationId,
        String targetAllocationId,
        DiscoveryNode targetNode,
        ShardId shardId,
        String file
    ) {
        super(replicationId, targetAllocationId, targetNode);
        this.fileName = file;
        this.shardId = shardId;
    }

    public GetFilesRequest(StreamInput in) throws IOException {
        super(in);
        this.fileName = in.readString();
        this.shardId = new ShardId(in);
//        this.filesToFetch = in.readList(StoreFileMetadata::new);
//        this.checkpoint = new ReplicationCheckpoint(in);
    }

    public String getFileName() {
        return fileName;
    }

//    public ReplicationCheckpoint getCheckpoint() {
//        return checkpoint;
//    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(fileName);
//        out.writeList(filesToFetch);
//        checkpoint.writeTo(out);
    }

    public ShardId getShardId() {
        return shardId;
    }
}
