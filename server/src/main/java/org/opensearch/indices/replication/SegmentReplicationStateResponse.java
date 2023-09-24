/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

public class SegmentReplicationStateResponse extends TransportResponse {

    private final Long seqNo;

    public SegmentReplicationStateResponse(Long seqNo) {
        this.seqNo = seqNo;
    }

    public SegmentReplicationStateResponse(StreamInput in) throws IOException {
        this.seqNo = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(seqNo);
    }
}
