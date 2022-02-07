/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.copy;

import org.opensearch.action.StepListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.segmentcopy.CheckpointInfoResponse;

import java.util.List;
import java.util.UUID;

public interface ReplicationSource {
    String name();

    void getCheckpointInfo(long replicationId, ReplicationCheckpoint checkpoint, StepListener<CheckpointInfoResponse> listener);

    void getFiles(long replicationId, ReplicationCheckpoint checkpoint, List<StoreFileMetadata> filesToFetch, StepListener<GetFilesResponse> listener);

    default void cancel() {}
}
