/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.SearcherFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class SegrepPrimaryNode extends PrimaryNode {
    public SegrepPrimaryNode(IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion, SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
        super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
    }

    @Override
    protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String, FileMetaData> files) throws IOException {

    }

    public long getPrimaryGen() {
        return primaryGen;
    }
}
