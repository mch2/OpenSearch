/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.segmentcopy;

import org.opensearch.index.shard.IndexShard;

public interface SegmentCopyService {
    public void pullSegments(long primaryTerm, long segmentInfosVersion, long seqNo, IndexShard shard);
}
