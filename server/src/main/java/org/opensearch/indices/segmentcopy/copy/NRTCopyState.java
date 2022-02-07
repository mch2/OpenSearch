/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.copy;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.NRTIndexCommit;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;

public class NRTCopyState extends AbstractRefCounted {

    private final Engine.IndexCommitRef commitRef;
    private final ReplicationCheckpoint checkpoint;
    private final Store.MetadataSnapshot metadataSnapshot;

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public Store.MetadataSnapshot getMetadataSnapshot() {
        return metadataSnapshot;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }

    private final byte[] infosBytes;

    public NRTCopyState(IndexShard shard) throws IOException {
        super("replication-nrt-state");
        this.commitRef = shard.getLatestNRTCommit();
        final NRTIndexCommit indexCommit = (NRTIndexCommit) commitRef.getIndexCommit();
        this.checkpoint = new ReplicationCheckpoint(shard.shardId(), shard.getOperationPrimaryTerm(), indexCommit.getGeneration(), shard.getLocalCheckpoint());
        this.metadataSnapshot = shard.store().getMetadata(indexCommit);
        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput tmpIndexOutput =
                 new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
            indexCommit.getInfos().write(tmpIndexOutput);
        }
        this.infosBytes = buffer.toArrayCopy();
    }


    @Override
    protected void closeInternal() {
        try {
            commitRef.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
