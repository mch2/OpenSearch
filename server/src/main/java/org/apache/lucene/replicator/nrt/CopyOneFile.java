/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.replicator.nrt;

import org.opensearch.action.StepListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.copy.GetFilesResponse;
import org.opensearch.indices.replication.copy.PrimaryShardReplicationSource;
import org.opensearch.indices.replication.copy.SegmentReplicationTarget;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CopyOneFile implements Closeable {

    private final ShardId shardId;
    public long bytesCopied;
    private final PrimaryShardReplicationSource replicationSource;
    private final ReplicaNode dest;
    public final String name;
    public final String tmpName;
    public final FileMetaData metaData;
    public final long bytesToCopy;
    private final AtomicBoolean result = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    public CopyOneFile(ShardId shardId,
                       ReplicaNode dest,
                       String name,
                       FileMetaData metaData,
                       PrimaryShardReplicationSource replicationSource) {
        this.shardId = shardId;
        this.dest = dest;
        this.name = name;
        this.replicationSource = replicationSource;
        this.metaData = metaData;
        this.tmpName = SegmentReplicationTarget.REPLICATION_PREFIX + name;
        this.bytesToCopy = metaData.length;
        dest.startCopyFile(name);
    }

    @Override
    public void close() throws IOException {
        dest.finishCopyFile(name);
    }

    public synchronized boolean visit() throws IOException {
        if (started.get() == false) {
            started.compareAndSet(false, true);
            StepListener<GetFilesResponse> listener = new StepListener<>();
            replicationSource.getFile(0, shardId, name, listener);
            listener.whenComplete(r -> {
                bytesCopied = metaData.length;
                result.set(true);
            }, listener::onFailure);
        }
        return result.get();
    }

    public long getBytesCopied() {
        return bytesCopied;
    }
}
