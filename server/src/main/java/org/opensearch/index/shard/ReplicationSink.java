/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.Term;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.ParsedDocument;

import java.util.List;

@PublicApi(since = "3.0.0")
public interface ReplicationSink {

    @PublicApi(since = "3.0.0")
    interface OperationDetails {
        String docId();
        Term uId();
        long seqNo();
        long primaryTerm();
    }

    @PublicApi(since = "3.0.0")
    record IndexingOperationDetails(String docId, Term uId, long seqNo, long primaryTerm, ParsedDocument parsedDoc) implements OperationDetails {
        @Override
        public String toString() {
            return "IndexingOperationDetails{" +
                "docId='" + docId + '\'' +
                ", uId=" + uId +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", parsedDoc=" + parsedDoc +
                '}';
        }
    }

    @PublicApi(since = "3.0.0")
    record DeleteOperationDetails(String docId, Term uId, long seqNo, long primaryTerm) implements OperationDetails {}

    // for each op in operationDetails:
    // create a map of namespace to operation to upload ie:
    // field 1 - List<Vec> <-- batch and upload these
    // field 2 - List<Vec>
    void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener);
}
