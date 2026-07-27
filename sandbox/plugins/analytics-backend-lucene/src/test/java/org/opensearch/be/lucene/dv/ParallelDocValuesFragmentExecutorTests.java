/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ParallelDocValuesFragmentExecutor}: multi-producer correctness (same multiset of
 * rows as sequential, order unspecified), backpressure (bounded queue), and the cancel/close leak
 * contract — close mid-stream must free every queued export back to the allocator, join all
 * producer threads, and run the lease cleanup exactly once.
 */
public class ParallelDocValuesFragmentExecutorTests extends OpenSearchTestCase {

    private static final Field V = new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Schema SCHEMA = new Schema(List.of(V));

    /** 8 committed segments x 50 docs, values seg*1000+i. */
    private static void writeSegments(Directory dir) throws Exception {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int seg = 0; seg < 8; seg++) {
                for (int i = 0; i < 50; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("v", seg * 1000L + i));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    private static ParallelDocValuesFragmentExecutor open(
        BufferAllocator alloc,
        IndexSearcher searcher,
        int batchSize,
        int parallelism,
        Runnable onClose
    ) throws Exception {
        return new ParallelDocValuesFragmentExecutor(
            alloc,
            searcher,
            new MatchAllDocsQuery(),
            SCHEMA,
            () -> new LuceneColumnBatchSource(List.of(new DvColumnSpec(V, DvColumnSpec.DecodeKind.NUMERIC_LONG)), batchSize),
            batchSize,
            parallelism,
            onClose
        );
    }

    private static List<Long> drain(ParallelDocValuesFragmentExecutor cursor, BufferAllocator alloc) throws Exception {
        List<Long> values = new ArrayList<>();
        long ptr;
        while ((ptr = cursor.next()) != 0) {
            try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(alloc); ArrowArray array = ArrowArray.wrap(ptr)) {
                Data.exportSchema(alloc, SCHEMA, null, ffiSchema);
                try (VectorSchemaRoot imported = Data.importVectorSchemaRoot(alloc, array, ffiSchema, null)) {
                    BigIntVector v = (BigIntVector) imported.getVector(0);
                    for (int i = 0; i < imported.getRowCount(); i++) {
                        values.add(v.get(i));
                    }
                }
            }
        }
        return values;
    }

    /**
     * Dictionary keyword mode under parallelism (the regression this guards: dictionary mode used to
     * force parallelism=1, serializing every string group-by). Each producer owns its own source and
     * per-batch dictionaries and exports a per-batch physical schema alongside the array; the consumer
     * imports with {@link ParallelDocValuesFragmentExecutor#currentSchemaPtr()} and the decoded terms
     * must equal the corpus multiset. A dropped schema pointer would make the importer read the Int32
     * index array as the advertised Utf8 and fail — so a green round-trip proves the plumbing.
     */
    public void testParallelDictionaryKeywordRoundTrip() throws Exception {
        Field k = new Field("k", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema advertised = new Schema(List.of(k));
        String[] palette = { "alpha", "bravo", "charlie", "delta" };
        List<String> expected = new ArrayList<>();
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int seg = 0; seg < 8; seg++) {
                    for (int i = 0; i < 50; i++) {
                        Document doc = new Document();
                        String term = palette[(seg + i) % palette.length];
                        doc.add(new SortedDocValuesField("k", new BytesRef(term)));
                        expected.add(term);
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                AtomicInteger closes = new AtomicInteger();
                ParallelDocValuesFragmentExecutor cursor = new ParallelDocValuesFragmentExecutor(
                    alloc,
                    searcher,
                    new MatchAllDocsQuery(),
                    advertised,
                    () -> new LuceneColumnBatchSource(
                        List.of(new DvColumnSpec(k, DvColumnSpec.DecodeKind.KEYWORD_ORD)),
                        16,
                        LuceneColumnBatchSource.KeywordEncoding.DICTIONARY,
                        alloc
                    ),
                    16,
                    4,
                    closes::incrementAndGet
                );
                List<String> values = new ArrayList<>();
                long ptr;
                while ((ptr = cursor.next()) != 0) {
                    long schemaPtr = cursor.currentSchemaPtr();
                    assertTrue("dictionary mode must ship a per-batch schema pointer", schemaPtr != 0);
                    try (
                        ArrowArray array = ArrowArray.wrap(ptr);
                        ArrowSchema schema = ArrowSchema.wrap(schemaPtr);
                        org.apache.arrow.c.CDataDictionaryProvider provider = new org.apache.arrow.c.CDataDictionaryProvider()
                    ) {
                        try (VectorSchemaRoot imported = Data.importVectorSchemaRoot(alloc, array, schema, provider)) {
                            IntVector indices = (IntVector) imported.getVector(0);
                            org.apache.arrow.vector.dictionary.Dictionary dict = provider.lookup(
                                indices.getField().getDictionary().getId()
                            );
                            VarCharVector dictVector = (VarCharVector) dict.getVector();
                            for (int i = 0; i < imported.getRowCount(); i++) {
                                assertFalse(indices.isNull(i));
                                values.add(new String(dictVector.get(indices.get(i)), java.nio.charset.StandardCharsets.UTF_8));
                            }
                        }
                    }
                }
                cursor.close();
                assertEquals("lease cleanup must run exactly once", 1, closes.get());
                values.sort(null);
                expected.sort(null);
                assertEquals("parallel dictionary multiset must equal the corpus", expected, values);
            }
        }
    }

    public void testParallelScanProducesSameMultisetAsSequential() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            writeSegments(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                AtomicInteger closes = new AtomicInteger();
                ParallelDocValuesFragmentExecutor cursor = open(alloc, searcher, 16, 4, closes::incrementAndGet);
                List<Long> values = drain(cursor, alloc);
                cursor.close();
                assertEquals(400, values.size());
                values.sort(null);
                List<Long> expected = new ArrayList<>();
                for (int seg = 0; seg < 8; seg++) {
                    for (int i = 0; i < 50; i++) {
                        expected.add(seg * 1000L + i);
                    }
                }
                expected.sort(null);
                assertEquals("parallel multiset must equal the full corpus", expected, values);
                assertEquals(400, cursor.docsMatched());
                assertEquals(400, cursor.docsRead());
                assertEquals("lease cleanup must run exactly once", 1, closes.get());
                cursor.close();
                assertEquals("close is idempotent", 1, closes.get());
            }
        }
    }

    /**
     * The leak gate: close the cursor after pulling only ONE batch while producers are blocked on
     * the full bounded queue. Every queued export must be released (allocator balance returns to
     * the single imported batch's zero after close), and all producer threads must exit.
     */
    public void testCloseMidStreamFreesQueuedExportsAndStopsProducers() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeSegments(dir);
            try (BufferAllocator alloc = new RootAllocator(); DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                AtomicInteger closes = new AtomicInteger();
                // Tiny batches → many batches per segment → producers saturate the bounded queue.
                ParallelDocValuesFragmentExecutor cursor = open(alloc, searcher, 8, 4, closes::incrementAndGet);
                long ptr = cursor.next();
                assertTrue("first pull must yield a batch", ptr != 0);
                try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(alloc); ArrowArray array = ArrowArray.wrap(ptr)) {
                    Data.exportSchema(alloc, SCHEMA, null, ffiSchema);
                    Data.importVectorSchemaRoot(alloc, array, ffiSchema, null).close();
                }
                // Abandon the stream mid-flight (the cancel path: JavaCursorStream::drop → leaf_close).
                cursor.close();
                assertEquals(1, closes.get());
                // All producer threads must be gone.
                assertBusy(() -> {
                    for (Thread t : Thread.getAllStackTraces().keySet()) {
                        assertFalse(
                            "producer thread leaked: " + t.getName(),
                            t.getName().startsWith("dv-leaf-segment-scan-") && t.isAlive()
                        );
                    }
                });
                // Allocator must be clean: every queued export was released on close. The
                // try-with-resources RootAllocator close below also asserts zero outstanding
                // (Arrow throws IllegalStateException on unreleased buffers).
                assertEquals("all exported buffers must be freed after close", 0L, alloc.getAllocatedMemory());
            }
        }
    }

    public void testProducerFailureSurfacesOnNext() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            writeSegments(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                ParallelDocValuesFragmentExecutor cursor = new ParallelDocValuesFragmentExecutor(
                    alloc,
                    searcher,
                    new MatchAllDocsQuery(),
                    SCHEMA,
                    () -> new ColumnBatchSource() {
                        @Override
                        public void decodeBatch(
                            org.apache.lucene.index.LeafReaderContext leaf,
                            int[] docIds,
                            int count,
                            VectorSchemaRoot out
                        ) {
                            throw new RuntimeException("boom");
                        }

                        @Override
                        public List<ColumnDecodeStats> decodeStats() {
                            return List.of();
                        }

                        @Override
                        public void close() {}
                    },
                    16,
                    2,
                    null
                );
                try {
                    Exception e = expectThrows(Exception.class, () -> {
                        long p;
                        while ((p = cursor.next()) != 0) {
                            ArrowArray.wrap(p).release();
                        }
                    });
                    assertTrue("failure must carry the producer error: " + e, e.getMessage().contains("boom"));
                } finally {
                    cursor.close();
                }
            }
        }
    }
}
