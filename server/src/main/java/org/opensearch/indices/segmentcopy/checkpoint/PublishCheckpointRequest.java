/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.checkpoint;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PublishCheckpointRequest extends BroadcastRequest<PublishCheckpointRequest> {

    private long primaryGen;
    private long seqNo;
    private long segmentInfoVersion;

    public PublishCheckpointRequest(String... indices) {
        super(indices);
    }

    public PublishCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        this.primaryGen = in.readLong();
        this.seqNo = in.readLong();
        this.segmentInfoVersion = in.readLong();
    }

    public PublishCheckpointRequest seqNo(long seqNo) {
        this.seqNo = seqNo;
        return this;
    }

    public PublishCheckpointRequest segmentInfoVersion(long version) {
        this.segmentInfoVersion = version;
        return this;
    }

    public PublishCheckpointRequest primaryGen(long primaryGen) {
        this.primaryGen = primaryGen;
        return this;
    }

    public long getPrimaryGen() { return primaryGen; }

    public long getSegmentInfoVersion() {
        return segmentInfoVersion;
    }

    public long getSeqNo() {
        return seqNo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(primaryGen);
        out.writeLong(seqNo);
        out.writeLong(segmentInfoVersion);
    }

    @Override
    public String toString() {
        return "PublishCheckpointRequest{" +
            "primaryGen=" + primaryGen +
            ", seqNo=" + seqNo +
            ", segmentInfoVersion=" + segmentInfoVersion +
            '}';
    }
}
