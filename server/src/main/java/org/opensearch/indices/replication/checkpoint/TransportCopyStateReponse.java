/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.lucene.replicator.nrt.CopyState;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportCopyStateReponse extends TransportResponse {

    private final CopyState copyState;

    public TransportCopyStateReponse(
        final CopyState copyState
    ) {
        this.copyState = copyState;
    }

    public TransportCopyStateReponse(StreamInput in) throws IOException {
        this.copyState = new CopyState(in.readMap(StreamInput::readString, this::readFileMetadata),
            in.readLong(),
            in.readLong(),
            in.readByteArray(),
            new HashSet<>(in.readStringList()),
            in.readLong(),
            null // infos is only non null on the primary node, we don't need to send anything.
            );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(copyState.files, StreamOutput::writeString, (s, o) -> writeFileMetadata(o, s));
        out.writeLong(copyState.version);
        out.writeLong(copyState.gen);
        out.writeByteArray(copyState.infosBytes);
        out.writeStringCollection(copyState.completedMergeFiles);
        out.writeLong(copyState.primaryGen);
    }

    public CopyState getCopyState() {
        return copyState;
    }

    private void writeFileMetadata(FileMetaData metaData, StreamOutput out) throws IOException {
        out.writeByteArray(metaData.header);
        out.writeByteArray(metaData.footer);
        out.writeVLong(metaData.length);
        out.writeVLong(metaData.checksum);
    }

    private FileMetaData readFileMetadata(StreamInput in) throws IOException {
        return new FileMetaData(
            in.readByteArray(),
            in.readByteArray(),
            in.readVLong(),
            in.readVLong()
        );
    }
}
