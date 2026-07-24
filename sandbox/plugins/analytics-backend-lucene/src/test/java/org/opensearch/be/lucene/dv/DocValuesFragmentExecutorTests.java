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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * End-to-end cursor tests for {@link DocValuesFragmentExecutor}: multi-segment scans with small
 * batches (a batch never spans segments), the Arrow C-Data export/import round-trip the Rust
 * {@code JavaCursorStream} performs, filtered (delegated-query) scans, counters, and close
 * releasing the lease exactly once.
 */
public class DocValuesFragmentExecutorTests extends OpenSearchTestCase {

    private static final Field V = new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null);
    private static final Schema SCHEMA = new Schema(List.of(V));

    /** Drain the cursor exactly like leaf_stream.rs: pull ptr, import with the known schema, repeat. */
    private static List<Long> drain(DocValuesFragmentExecutor cursor, BufferAllocator alloc) throws Exception {
        List<Long> values = new ArrayList<>();
        long ptr;
        while ((ptr = cursor.next()) != 0) {
            try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(alloc); ArrowArray array = ArrowArray.wrap(ptr)) {
                Data.exportSchema(alloc, SCHEMA, null, ffiSchema);
                try (VectorSchemaRoot imported = Data.importVectorSchemaRoot(alloc, array, ffiSchema, null)) {
                    BigIntVector v = (BigIntVector) imported.getVector(0);
                    for (int i = 0; i < imported.getRowCount(); i++) {
                        values.add(v.isNull(i) ? null : v.get(i));
                    }
                }
            }
        }
        return values;
    }

    public void testMultiSegmentScanBatchesNeverSpanSegments() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            // 3 segments of 10 docs each (commit per segment).
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int seg = 0; seg < 3; seg++) {
                    for (int i = 0; i < 10; i++) {
                        Document doc = new Document();
                        doc.add(new NumericDocValuesField("v", seg * 100L + i));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                AtomicBoolean released = new AtomicBoolean(false);
                int batchSize = 4; // forces multiple batches per segment + a short tail batch
                DocValuesFragmentExecutor cursor = new DocValuesFragmentExecutor(
                    alloc,
                    searcher,
                    new MatchAllDocsQuery(),
                    SCHEMA,
                    new LuceneColumnBatchSource(List.of(new DvColumnSpec(V, DvColumnSpec.DecodeKind.NUMERIC_LONG)), batchSize),
                    batchSize,
                    () -> released.set(true)
                );
                List<Long> values = drain(cursor, alloc);
                assertEquals(30, values.size());
                for (int seg = 0; seg < 3; seg++) {
                    for (int i = 0; i < 10; i++) {
                        assertEquals(Long.valueOf(seg * 100L + i), values.get(seg * 10 + i));
                    }
                }
                // 10 docs / batchSize 4 → 3 batches per segment (4+4+2): a batch never spans segments.
                assertEquals(9, cursor.batchesEmitted());
                assertEquals(30, cursor.docsMatched());
                assertEquals(30, cursor.docsRead());
                assertTrue(cursor.bytesEmitted() > 0);
                assertFalse("lease must not release before close", released.get());
                cursor.close();
                assertTrue("close must release the lease", released.get());
                cursor.close(); // idempotent
            }
        }
    }

    public void testFilteredScanCountsSelectivity() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint("v", i));
                    doc.add(new NumericDocValuesField("v", i));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                DocValuesFragmentExecutor cursor = new DocValuesFragmentExecutor(
                    alloc,
                    searcher,
                    LongPoint.newRangeQuery("v", 90, Long.MAX_VALUE),
                    SCHEMA,
                    new LuceneColumnBatchSource(List.of(new DvColumnSpec(V, DvColumnSpec.DecodeKind.NUMERIC_LONG)), 8192),
                    8192,
                    null
                );
                try {
                    List<Long> values = drain(cursor, alloc);
                    assertEquals(10, values.size());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(Long.valueOf(90 + i), values.get(i));
                    }
                    assertEquals("docsRead is the examined universe", 100, cursor.docsRead());
                    assertEquals("docsMatched is scorer-emitted", 10, cursor.docsMatched());
                } finally {
                    cursor.close();
                }
            }
        }
    }

    public void testEmptyResultReturnsEosImmediately() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); BufferAllocator alloc = new RootAllocator()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new LongPoint("v", 1));
                doc.add(new NumericDocValuesField("v", 1));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                DocValuesFragmentExecutor cursor = new DocValuesFragmentExecutor(
                    alloc,
                    searcher,
                    LongPoint.newExactQuery("v", 999),
                    SCHEMA,
                    new LuceneColumnBatchSource(List.of(new DvColumnSpec(V, DvColumnSpec.DecodeKind.NUMERIC_LONG)), 8192),
                    8192,
                    null
                );
                try {
                    assertEquals(0L, cursor.next());
                    assertEquals(0L, cursor.next()); // stays EOS
                    assertEquals(0, cursor.docsMatched());
                    assertEquals(1, cursor.docsRead());
                } finally {
                    cursor.close();
                }
            }
        }
    }
}
