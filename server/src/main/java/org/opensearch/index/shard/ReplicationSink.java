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
    abstract class OperationDetails {
        private String docId;
        private Term uId;
        private long seqNo;
        private long primaryTerm;

        public OperationDetails(String docId, Term uId, long seqNo, long primaryTerm) {
            this.docId = docId;
            this.uId = uId;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }

        public String docId() {
            return docId;
        }

        public Term uId() {
            return uId;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }
    }

    @PublicApi(since = "3.0.0")
    class IndexingOperationDetails extends OperationDetails {

        private ParsedDocument parsedDoc;

        public IndexingOperationDetails(String docId, Term uId, long seqNo, long primaryTerm, ParsedDocument parsedDoc) {
            super(docId, uId, seqNo, primaryTerm);
            this.parsedDoc = parsedDoc;
        }

        public ParsedDocument parsedDoc() {
            return parsedDoc;
        }
    }

    @PublicApi(since = "3.0.0")
    class DeleteOperationDetails extends OperationDetails {
        public DeleteOperationDetails(String docId, Term uId, long seqNo, long primaryTerm) {
            super(docId, uId, seqNo, primaryTerm);
        }
    }

    // for each op in operationDetails:
    // create a map of namespace to operation to upload ie:
    // field 1 - List<Vec> <-- batch and upload these
    // field 2 - List<Vec>
    void acceptBatch(ShardId shardId, List<OperationDetails> operationDetails, ActionListener<Long> listener);
}
