/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

public class SegmentReplicationStateRequest extends ReplicationRequest<SegmentReplicationStateRequest> {

    public long getWaitForSeqNo() {
        return waitForSeqNo;
    }

    private final long waitForSeqNo;

    public SegmentReplicationStateRequest(ShardId shardId, long waitForSeqNo) {
        super(shardId);
        this.waitForSeqNo = waitForSeqNo;
    }

    public SegmentReplicationStateRequest(StreamInput in) throws IOException {
        super(in);
        this.waitForSeqNo = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(waitForSeqNo);
    }

    @Override
    public String toString() {
        return "SegmentReplicationStateRequest{" +
            "waitForSeqNo=" + waitForSeqNo +
            ", shardId=" + shardId +
            '}';
    }
}
