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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
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
import org.apache.lucene.util.NumericUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decode-correctness tests for {@link LuceneColumnBatchSource} (spec test suite 1): per type,
 * nulls, deleted docs (liveDocs respected via the scorer), multi-segment scans, and the
 * bulk-vs-fallback counter split.
 */
public class LuceneColumnBatchSourceTests extends OpenSearchTestCase {

    private static final Field LONG_FIELD = new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Field DOUBLE_FIELD = new Field(
        "d",
        FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
        null
    );
    private static final Field KEYWORD_FIELD = new Field("k", FieldType.nullable(new ArrowType.Utf8()), null);

    /**
     * Collect all live docids of a segment via a MatchAll scorer + liveDocs check — the executor's
     * selection path (Weight.scorer does NOT filter deletions; the executor checks liveDocs).
     */
    private static int[] liveDocs(IndexSearcher searcher, LeafReaderContext leaf) throws IOException {
        Weight w = searcher.createWeight(searcher.rewrite(new MatchAllDocsQuery()), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer s = w.scorer(leaf);
        org.apache.lucene.util.Bits live = leaf.reader().getLiveDocs();
        List<Integer> ids = new ArrayList<>();
        if (s != null) {
            DocIdSetIterator it = s.iterator();
            for (int d = it.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                if (live == null || live.get(d)) {
                    ids.add(d);
                }
            }
        }
        return ids.stream().mapToInt(Integer::intValue).toArray();
    }

    public void testLongDecodeDenseTakesBulkPath() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("v", i * 7L));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = liveDocs(searcher, leaf);
                Schema schema = new Schema(List.of(LONG_FIELD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(DvColumnSpecTestUtil.numericLong(LONG_FIELD), docs.length);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    root.allocateNew();
                    source.decodeBatch(leaf, docs, docs.length, root);
                    root.setRowCount(docs.length);
                    BigIntVector v = (BigIntVector) root.getVector(0);
                    for (int i = 0; i < docs.length; i++) {
                        assertFalse("row " + i + " should not be null", v.isNull(i));
                        assertEquals(i * 7L, v.get(i));
                    }
                }
                // Dense column, whole-batch run → the bulk path must have engaged.
                ColumnBatchSource.ColumnDecodeStats stats = source.decodeStats().getFirst();
                assertEquals("bulk path should serve the dense batch", 1L, stats.bulkDecodeBatches());
                assertEquals(0L, stats.perDocFallbackBatches());
            }
        }
    }

    public void testLongDecodeSparseFallsBackWithNulls() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    if (i % 3 != 0) { // every third doc missing the field
                        doc.add(new NumericDocValuesField("v", i));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = liveDocs(searcher, leaf);
                Schema schema = new Schema(List.of(LONG_FIELD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(DvColumnSpecTestUtil.numericLong(LONG_FIELD), docs.length);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    root.allocateNew();
                    source.decodeBatch(leaf, docs, docs.length, root);
                    root.setRowCount(docs.length);
                    BigIntVector v = (BigIntVector) root.getVector(0);
                    for (int i = 0; i < docs.length; i++) {
                        if (i % 3 == 0) {
                            assertTrue("row " + i + " must be null (advanceExact miss)", v.isNull(i));
                        } else {
                            assertEquals(i, v.get(i));
                        }
                    }
                }
                ColumnBatchSource.ColumnDecodeStats stats = source.decodeStats().getFirst();
                assertEquals("sparse batch must NOT take the bulk path", 0L, stats.bulkDecodeBatches());
                assertEquals(1L, stats.perDocFallbackBatches());
            }
        }
    }

    public void testDoubleSortableBitsDecode() throws Exception {
        double[] values = { 0.0, -1.5, 42.25, Double.MAX_VALUE, -0.75 };
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (double value : values) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("d", NumericUtils.doubleToSortableLong(value)));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = liveDocs(searcher, leaf);
                Schema schema = new Schema(List.of(DOUBLE_FIELD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(
                    List.of(new DvColumnSpec(DOUBLE_FIELD, DvColumnSpec.DecodeKind.NUMERIC_SORTABLE_DOUBLE)),
                    docs.length
                );
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    root.allocateNew();
                    source.decodeBatch(leaf, docs, docs.length, root);
                    root.setRowCount(docs.length);
                    Float8Vector v = (Float8Vector) root.getVector(0);
                    for (int i = 0; i < values.length; i++) {
                        assertEquals(values[i], v.get(i), 0.0);
                    }
                }
            }
        }
    }

    public void testKeywordDecodeWithNulls() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                String[] terms = { "apple", null, "banana", "apple", null };
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
                int[] docs = liveDocs(searcher, leaf);
                Schema schema = new Schema(List.of(KEYWORD_FIELD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(
                    List.of(new DvColumnSpec(KEYWORD_FIELD, DvColumnSpec.DecodeKind.KEYWORD_ORD)),
                    docs.length
                );
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    root.allocateNew();
                    source.decodeBatch(leaf, docs, docs.length, root);
                    root.setRowCount(docs.length);
                    VarCharVector v = (VarCharVector) root.getVector(0);
                    assertEquals("apple", new String(v.get(0), StandardCharsets.UTF_8));
                    assertTrue(v.isNull(1));
                    assertEquals("banana", new String(v.get(2), StandardCharsets.UTF_8));
                    assertEquals("apple", new String(v.get(3), StandardCharsets.UTF_8));
                    assertTrue(v.isNull(4));
                }
            }
        }
    }

    public void testDeletedDocsExcludedByScorer() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "id" + i, org.apache.lucene.document.Field.Store.NO));
                    doc.add(new NumericDocValuesField("v", i));
                    writer.addDocument(doc);
                }
                writer.commit();
                writer.deleteDocuments(new org.apache.lucene.index.Term("id", "id7"));
                writer.deleteDocuments(new org.apache.lucene.index.Term("id", "id13"));
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(18, reader.numDocs());
                IndexSearcher searcher = new IndexSearcher(reader);
                List<Long> decoded = new ArrayList<>();
                for (LeafReaderContext leaf : reader.leaves()) {
                    int[] docs = liveDocs(searcher, leaf);
                    if (docs.length == 0) {
                        continue;
                    }
                    Schema schema = new Schema(List.of(LONG_FIELD));
                    LuceneColumnBatchSource source = new LuceneColumnBatchSource(DvColumnSpecTestUtil.numericLong(LONG_FIELD), docs.length);
                    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                        root.allocateNew();
                        source.decodeBatch(leaf, docs, docs.length, root);
                        root.setRowCount(docs.length);
                        BigIntVector v = (BigIntVector) root.getVector(0);
                        for (int i = 0; i < docs.length; i++) {
                            decoded.add(v.get(i));
                        }
                    }
                }
                assertEquals(18, decoded.size());
                assertFalse("deleted doc 7 must not decode", decoded.contains(7L));
                assertFalse("deleted doc 13 must not decode", decoded.contains(13L));
            }
        }
    }

    public void testMultiValuedFieldRejectedWithTypedError() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("v", 1));
                doc.add(new SortedNumericDocValuesField("v", 2));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = liveDocs(searcher, leaf);
                Schema schema = new Schema(List.of(LONG_FIELD));
                LuceneColumnBatchSource source = new LuceneColumnBatchSource(DvColumnSpecTestUtil.numericLong(LONG_FIELD), docs.length);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    root.allocateNew();
                    var e = expectThrows(
                        org.opensearch.analytics.spi.DocValuesLeafUnsupportedException.class,
                        () -> source.decodeBatch(leaf, docs, docs.length, root)
                    );
                    assertEquals(org.opensearch.analytics.spi.DocValuesLeafUnsupportedException.Reason.MULTI_VALUED, e.reason());
                }
            }
        }
    }

    /** Small helper so tests read declaratively. */
    static class DvColumnSpecTestUtil {
        static List<DvColumnSpec> numericLong(Field field) {
            return List.of(new DvColumnSpec(field, DvColumnSpec.DecodeKind.NUMERIC_LONG));
        }
    }
}
