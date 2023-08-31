/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class ReplicationStats implements ToXContentFragment, Writeable {

    public long maxBytesBehind;
    public long maxReplicationLag;
    public long totalBytesBehind;

    public ReplicationStats(long maxBytesBehind, long maxReplicationLag, long totalBytesBehind) {
        this.maxBytesBehind = maxBytesBehind;
        this.maxReplicationLag = maxReplicationLag;
        this.totalBytesBehind = totalBytesBehind;
    }

    public ReplicationStats(StreamInput in) throws IOException {
        this.maxBytesBehind = in.readVLong();
        this.maxBytesBehind = in.readVLong();
        this.maxBytesBehind = in.readVLong();
    }

    public void add(ReplicationStats other) {
        if (other == null) {
            return;
        }
        maxBytesBehind = Math.max(other.maxBytesBehind, maxBytesBehind);
        maxReplicationLag = Math.max(other.maxReplicationLag, maxReplicationLag);
        totalBytesBehind += other.totalBytesBehind;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(maxBytesBehind);
        out.writeVLong(maxReplicationLag);
        out.writeVLong(totalBytesBehind);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REPLICATION);
        builder.field(Fields.MAX_BYTES_BEHIND, new ByteSizeValue(maxBytesBehind).toString());
        builder.field(Fields.TOTAL_BYTES_BEHIND, new ByteSizeValue(totalBytesBehind).toString());
        builder.field(Fields.MAX_REPLICATION_LAG, new TimeValue(maxReplicationLag));
        builder.endObject();
        return builder;
    }

    /**
     * Fields for replication statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String REPLICATION = "replication";
        static final String MAX_BYTES_BEHIND = "max_bytes_behind";
        static final String TOTAL_BYTES_BEHIND = "total_bytes_behind";
        static final String MAX_REPLICATION_LAG = "max_replication_lag";
    }
}
