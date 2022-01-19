/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.segmentcopy.copy;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoveryTransportRequest;

import java.io.IOException;

public final class ReceiveSegmentInfosBytesRequest extends RecoveryTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final long gen;
    private final long version;
    private final byte[] infosBytes;

    public long getRecoveryId() {
        return recoveryId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getGen() {
        return gen;
    }

    public long getVersion() {
        return version;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }

    public ReceiveSegmentInfosBytesRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readVLong();
        shardId = new ShardId(in);
        gen = in.readVLong();
        version = in.readVLong();
        int infosBytesLength = in.readVInt();
        infosBytes = new byte[infosBytesLength];
        in.readBytes(infosBytes, 0, infosBytesLength);
    }

    ReceiveSegmentInfosBytesRequest(final long recoveryId,
                                    final long requestSeqNo,
                                    final ShardId shardId,
                                    final long gen,
                                    final long version,
                                    final byte[] infosBytes) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.gen = gen;
        this.version = version;
        this.infosBytes = infosBytes;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(recoveryId);
        shardId.writeTo(out);
        out.writeVLong(gen);
        out.writeVLong(version);
        out.writeVInt(infosBytes.length);
        out.writeBytes(infosBytes, 0, infosBytes.length);
    }

}
