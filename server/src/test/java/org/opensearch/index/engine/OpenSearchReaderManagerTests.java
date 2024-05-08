/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90TermVectorsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94FieldInfosFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99SegmentInfoFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow;

import static org.opensearch.index.engine.NRTReplicationReaderManager.unwrapStandardReader;

public class OpenSearchReaderManagerTests extends OpenSearchTestCase {

    public void testReaderManagerMetadata() throws IOException {
        try (BaseDirectoryWrapper dir = newDirectory()) {
            final IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(new Lucene99Codec());
            IndexWriter writer = new IndexWriter(dir, iwc);
            final OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
                DirectoryReader.open(writer),
                new ShardId("nvm", UUID.randomUUID().toString(), 0)
            );
            try (OpenSearchReaderManager openSearchReaderManager = new OpenSearchReaderManager(reader)) {
                writer.addDocument(new Document());
                openSearchReaderManager.maybeRefresh();
                try (OpenSearchDirectoryReader r = openSearchReaderManager.acquire()) {
                    assertEquals(1, r.numDocs());
                    StandardDirectoryReader standardDirectoryReader = unwrapStandardReader(r);
                    assertCodec(standardDirectoryReader.getSegmentInfos().asList().get(0));
                }
                writer.addDocument(new Document());
                openSearchReaderManager.maybeRefresh();
                try (OpenSearchDirectoryReader r = openSearchReaderManager.acquire()) {
                    assertEquals(2, r.numDocs());
                    StandardDirectoryReader standardDirectoryReader = unwrapStandardReader(r);
                    assertCodec(standardDirectoryReader.getSegmentInfos().asList().get(0));
                    assertCodec(standardDirectoryReader.getSegmentInfos().asList().get(1));
                    r.leaves().stream().forEach(l -> {
                        System.out.println(l.reader());
                    });
                }
            }
            writer.close();
        }
    }

    void assertCodec(SegmentCommitInfo segmentCommitInfo) {
        assertEquals("DummyCodec", segmentCommitInfo.info.getCodec().getName());
    }
}
