/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Dictionary keyword mode (dv.keyword_encoding=dictionary): per-batch dictionaries hold only the
 * batch's DISTINCT terms, indices resolve through them, nulls stay null, and the physical schema
 * swaps the keyword column for a dictionary-encoded field.
 */
public class DictionaryKeywordDecodeTests extends OpenSearchTestCase {

    private static final Field K = new Field("k", FieldType.nullable(new ArrowType.Utf8()), null);
    private static final Schema ADVERTISED = new Schema(List.of(K));

    private static int[] allDocs(IndexSearcher searcher, LeafReaderContext leaf) throws IOException {
        Weight w = searcher.createWeight(searcher.rewrite(new MatchAllDocsQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer s = w.scorer(leaf);
        List<Integer> ids = new ArrayList<>();
        DocIdSetIterator it = s.iterator();
        for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
            ids.add(d);
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }

    public void testDictionaryDecodeRoundTrip() throws Exception {
        String[] terms = { "b", "a", null, "b", "c", "a", "b", null };
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (String term : terms) {
                    Document doc = new Document();
                    if (term != null) {
                        doc.add(new SortedDocValuesField("k", new BytesRef(term)));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = allDocs(searcher, leaf);
                List<DvColumnSpec> specs = List.of(new DvColumnSpec(K, DvColumnSpec.DecodeKind.KEYWORD_ORD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(
                    specs,
                    docs.length,
                    LuceneColumnBatchSource.KeywordEncoding.DICTIONARY,
                    alloc
                );
                Schema physical = source.physicalSchema(ADVERTISED);
                assertNotNull("keyword column must be dictionary-encoded", physical.getFields().getFirst().getDictionary());
                try (VectorSchemaRoot root = VectorSchemaRoot.create(physical, alloc)) {
                    root.allocateNew();
                    source.decodeBatch(leaf, docs, docs.length, root);
                    root.setRowCount(docs.length);
                    IntVector indices = (IntVector) root.getVector(0);
                    Dictionary dict = source.dictionaryProvider().lookup(0);
                    assertNotNull(dict);
                    VarCharVector dictVector = (VarCharVector) dict.getVector();
                    // 3 distinct non-null terms in the batch.
                    assertEquals(3, dictVector.getValueCount());
                    for (int i = 0; i < terms.length; i++) {
                        if (terms[i] == null) {
                            assertTrue("row " + i + " must be null", indices.isNull(i));
                        } else {
                            String resolved = new String(dictVector.get(indices.get(i)), StandardCharsets.UTF_8);
                            assertEquals("row " + i, terms[i], resolved);
                        }
                    }
                }
                source.close();
            }
        }
    }
}
