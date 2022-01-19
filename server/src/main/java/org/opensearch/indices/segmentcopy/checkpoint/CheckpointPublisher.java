/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy.checkpoint;

/**
 * Used to publish new checkpoints that will be consumed by replica shards.
 */
public interface CheckpointPublisher {
    void publish(long primaryTerm, long segmentInfosVersion, long seqNo);
}
