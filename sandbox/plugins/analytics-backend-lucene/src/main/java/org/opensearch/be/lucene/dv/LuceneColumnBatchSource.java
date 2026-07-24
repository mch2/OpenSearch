/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.analytics.spi.DocValuesLeafUnsupportedException;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PoC-1 {@link ColumnBatchSource}: decodes Lucene doc values into Arrow vectors.
 *
 * <p>Numerics take the codec BULK path when the batch is a dense run — Lucene 10.5's
 * {@code NumericDocValues#longValues(size, docs[], values[], default)} has producer fast paths that
 * skip per-doc virtual dispatch. The bulk API cannot report missing values, so it is used only when
 * {@code advanceExact(docs[0]) && docIDRunEnd() > docs[count-1]} proves every batch doc has a value;
 * otherwise the batch falls back to per-doc {@code advanceExact} (nulls handled exactly). The
 * bulk-vs-fallback split is counted per column — without it, "bulk decode working" and "silently on
 * the slow path" are indistinguishable.
 *
 * <p>Keywords decode per-doc via {@code SortedDocValues.ordValue()+lookupOrd} (Lucene 10.5 ships no
 * bulk ordinal API; that path exists only on the codec fork — see the spec addendum).
 *
 * <p>Doc-values iterators are forward-only, so this source caches one iterator per column per
 * segment and requires ascending doc IDs / ascending batches; it is single-threaded by contract
 * (thread-per-segment above creates one source per thread).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneColumnBatchSource implements ColumnBatchSource {

    /** Enables the ascending-docid assertion in dev builds (-ea); ordering is load-bearing. */
    private static boolean assertAscending(int[] docIds, int count) {
        for (int i = 1; i < count; i++) {
            if (docIds[i] <= docIds[i - 1]) {
                throw new AssertionError("doc IDs not strictly ascending at " + i + ": " + docIds[i - 1] + " -> " + docIds[i]);
            }
        }
        return true;
    }

    /** Keyword materialization mode (the deliberate A/B — see the spec's "ordinal question"). */
    public enum KeywordEncoding {
        /** Materialize term bytes per row (simple, correct baseline; the default). */
        UTF8,
        /**
         * Emit {@code DictionaryArray(Int32 -> Utf8)}: per-doc SEGMENT ordinals are re-encoded
         * against a PER-BATCH dictionary built from the batch's distinct ords (segments' term
         * dictionaries differ, so segment ords can't be used directly across batches; per-batch
         * resolution costs one lookupOrd per DISTINCT term per batch instead of one per row —
         * the win grows with duplication, exactly the group-by shape item 9 cares about).
         */
        DICTIONARY
    }

    private final List<DvColumnSpec> specs;
    private final long[] scratch;
    private final int[] docsScratch;
    private final KeywordEncoding keywordEncoding;
    /** Allocator for per-batch dictionary vectors (dictionary mode only; null in utf8 mode). */
    private final org.apache.arrow.memory.BufferAllocator dictionaryAllocator;

    // Per-column iterator state, valid for the CURRENT segment only.
    private LeafReaderContext currentLeaf;
    private final ColumnState[] columns;

    // Dictionary mode: the current batch's dictionaries (rebuilt each decodeBatch; freed on the next).
    private org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider currentDictionaries;

    private static final class ColumnState {
        NumericDocValues numeric;          // singleton numeric (bulk-capable)
        SortedNumericDocValues sortedNumeric; // multi-shaped numeric (checked for cardinality 1)
        SortedDocValues sorted;            // singleton keyword ords
        SortedSetDocValues sortedSet;      // multi-shaped keyword (checked for cardinality 1)
        long bulkBatches;
        long fallbackBatches;
        long nanos;
    }

    public LuceneColumnBatchSource(List<DvColumnSpec> specs, int batchSize) {
        this(specs, batchSize, KeywordEncoding.UTF8, null);
    }

    public LuceneColumnBatchSource(
        List<DvColumnSpec> specs,
        int batchSize,
        KeywordEncoding keywordEncoding,
        org.apache.arrow.memory.BufferAllocator dictionaryAllocator
    ) {
        if (keywordEncoding == KeywordEncoding.DICTIONARY && dictionaryAllocator == null) {
            throw new IllegalArgumentException("dictionary keyword mode requires an allocator for dictionary vectors");
        }
        this.specs = specs;
        this.scratch = new long[batchSize];
        this.docsScratch = new int[batchSize];
        this.keywordEncoding = keywordEncoding;
        this.dictionaryAllocator = dictionaryAllocator;
        this.columns = new ColumnState[specs.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = new ColumnState();
        }
    }

    @Override
    public org.apache.arrow.vector.types.pojo.Schema physicalSchema(org.apache.arrow.vector.types.pojo.Schema advertised) {
        if (keywordEncoding == KeywordEncoding.UTF8) {
            return advertised;
        }
        // Swap keyword columns for dictionary-encoded fields: Int32 indices -> Utf8 values.
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>(advertised.getFields().size());
        for (int c = 0; c < specs.size(); c++) {
            org.apache.arrow.vector.types.pojo.Field f = advertised.getFields().get(c);
            if (specs.get(c).kind() == DvColumnSpec.DecodeKind.KEYWORD_ORD) {
                org.apache.arrow.vector.types.pojo.DictionaryEncoding enc = new org.apache.arrow.vector.types.pojo.DictionaryEncoding(
                    c, // dictionary id = column ordinal (unique per fragment)
                    false,
                    new ArrowType.Int(32, true)
                );
                // Arrow Java convention for encoded vectors: the FIELD carries the INDEX type
                // (Int32) plus the DictionaryEncoding; the VALUE type (Utf8) lives on the
                // dictionary vector in the provider. (Mirrors DictionaryEncoder.encode.)
                fields.add(
                    new org.apache.arrow.vector.types.pojo.Field(
                        f.getName(),
                        new org.apache.arrow.vector.types.pojo.FieldType(f.isNullable(), new ArrowType.Int(32, true), enc),
                        null
                    )
                );
            } else {
                fields.add(f);
            }
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    @Override
    public org.apache.arrow.vector.dictionary.DictionaryProvider dictionaryProvider() {
        return currentDictionaries;
    }

    @Override
    public void decodeBatch(LeafReaderContext leaf, int[] docIds, int count, VectorSchemaRoot out) throws IOException {
        assert assertAscending(docIds, count);
        if (leaf != currentLeaf) {
            openSegment(leaf);
        }
        releaseCurrentDictionaries();
        for (int c = 0; c < specs.size(); c++) {
            DvColumnSpec spec = specs.get(c);
            ColumnState state = columns[c];
            FieldVector vector = out.getVector(c);
            long start = System.nanoTime();
            if (spec.kind() == DvColumnSpec.DecodeKind.KEYWORD_ORD) {
                if (keywordEncoding == KeywordEncoding.DICTIONARY) {
                    decodeKeywordDictionary(c, state, docIds, count, (IntVector) vector);
                } else {
                    decodeKeyword(state, docIds, count, vector);
                }
            } else {
                decodeNumeric(spec.kind(), state, docIds, count, vector);
            }
            state.nanos += System.nanoTime() - start;
        }
    }

    private void releaseCurrentDictionaries() {
        if (currentDictionaries != null) {
            currentDictionaries.close();
            currentDictionaries = null;
        }
    }

    /** (Re)open the per-column iterators for a new segment — forward-only iterators can't rewind. */
    private void openSegment(LeafReaderContext leaf) throws IOException {
        for (int c = 0; c < specs.size(); c++) {
            DvColumnSpec spec = specs.get(c);
            ColumnState state = columns[c];
            String field = spec.arrowField().getName();
            state.numeric = null;
            state.sortedNumeric = null;
            state.sorted = null;
            state.sortedSet = null;
            if (spec.kind() == DvColumnSpec.DecodeKind.KEYWORD_ORD) {
                SortedSetDocValues sortedSet = DocValues.getSortedSet(leaf.reader(), field);
                SortedDocValues singleton = DocValues.unwrapSingleton(sortedSet);
                if (singleton != null) {
                    state.sorted = singleton;
                } else {
                    state.sortedSet = sortedSet;
                }
            } else {
                SortedNumericDocValues sortedNumeric = DocValues.getSortedNumeric(leaf.reader(), field);
                NumericDocValues singleton = DocValues.unwrapSingleton(sortedNumeric);
                if (singleton != null) {
                    state.numeric = singleton;
                } else {
                    state.sortedNumeric = sortedNumeric;
                }
            }
        }
        currentLeaf = leaf;
    }

    // ── Numeric decode ──

    private void decodeNumeric(DvColumnSpec.DecodeKind kind, ColumnState state, int[] docIds, int count, FieldVector vector)
        throws IOException {
        if (state.numeric != null) {
            NumericDocValues dv = state.numeric;
            // Dense-run probe (see NumericDocValues#longValues javadoc): if the doc-values run
            // covers the whole batch, every doc has a value — the bulk API is exact and fast.
            if (count > 0 && dv.docID() <= docIds[0] && dv.advanceExact(docIds[0]) && dv.docIDRunEnd() > docIds[count - 1]) {
                dv.longValues(count, docIds, scratch, 0L);
                for (int i = 0; i < count; i++) {
                    writeNumeric(kind, vector, i, scratch[i]);
                }
                state.bulkBatches++;
                return;
            }
            state.fallbackBatches++;
            for (int i = 0; i < count; i++) {
                if (dv.docID() <= docIds[i] && dv.advanceExact(docIds[i])) {
                    writeNumeric(kind, vector, i, dv.longValue());
                } else {
                    vector.setNull(i);
                }
            }
            return;
        }
        // Multi-shaped SORTED_NUMERIC: allowed only when every matching doc carries <=1 value.
        SortedNumericDocValues dv = state.sortedNumeric;
        state.fallbackBatches++;
        for (int i = 0; i < count; i++) {
            if (dv.advanceExact(docIds[i])) {
                if (dv.docValueCount() > 1) {
                    throw new DocValuesLeafUnsupportedException(
                        DocValuesLeafUnsupportedException.Reason.MULTI_VALUED,
                        "field [" + vector.getName() + "] has " + dv.docValueCount() + " values at doc " + docIds[i]
                    );
                }
                writeNumeric(kind, vector, i, dv.nextValue());
            } else {
                vector.setNull(i);
            }
        }
    }

    private static void writeNumeric(DvColumnSpec.DecodeKind kind, FieldVector vector, int row, long raw) {
        switch (kind) {
            case NUMERIC_SORTABLE_DOUBLE -> ((Float8Vector) vector).setSafe(row, NumericUtils.sortableLongToDouble(raw));
            case NUMERIC_SORTABLE_FLOAT -> ((Float4Vector) vector).setSafe(row, NumericUtils.sortableIntToFloat((int) raw));
            case NUMERIC_LONG -> writeLong(vector, row, raw);
            default -> throw new IllegalStateException("keyword kind routed to numeric decode");
        }
    }

    /** Raw-long write dispatched on the PROJECTED Arrow type (which the coordinator planned). */
    private static void writeLong(FieldVector vector, int row, long value) {
        if (vector instanceof BigIntVector v) {
            v.setSafe(row, value);
        } else if (vector instanceof IntVector v) {
            v.setSafe(row, (int) value);
        } else if (vector instanceof SmallIntVector v) {
            v.setSafe(row, (short) value);
        } else if (vector instanceof TinyIntVector v) {
            v.setSafe(row, (byte) value);
        } else if (vector instanceof BitVector v) {
            v.setSafe(row, value == 0 ? 0 : 1);
        } else if (vector instanceof TimeStampVector v) {
            // Doc values store epoch millis; rescale to the planned Timestamp unit.
            v.setSafe(row, rescaleMillis(value, ((ArrowType.Timestamp) v.getField().getType()).getUnit()));
        } else if (vector instanceof Float8Vector v) {
            // Planner widened an integer column to double (e.g. avg input) — value-cast.
            v.setSafe(row, (double) value);
        } else {
            throw new DocValuesLeafUnsupportedException(
                DocValuesLeafUnsupportedException.Reason.UNSUPPORTED_FIELD_TYPE,
                "no long-decode into Arrow vector " + vector.getClass().getSimpleName() + " for [" + vector.getName() + "]"
            );
        }
    }

    private static long rescaleMillis(long millis, TimeUnit unit) {
        return switch (unit) {
            case SECOND -> millis / 1000L;
            case MILLISECOND -> millis;
            case MICROSECOND -> millis * 1000L;
            case NANOSECOND -> millis * 1_000_000L;
        };
    }

    // ── Keyword decode (utf8 mode; dictionary mode is a follow-on flag) ──

    private void decodeKeyword(ColumnState state, int[] docIds, int count, FieldVector vector) throws IOException {
        state.fallbackBatches++; // no bulk ordinal API at Lucene 10.5 — always the per-doc path
        if (state.sorted != null) {
            SortedDocValues dv = state.sorted;
            for (int i = 0; i < count; i++) {
                if (dv.docID() <= docIds[i] && dv.advanceExact(docIds[i])) {
                    writeUtf8(vector, i, dv.lookupOrd(dv.ordValue()));
                } else {
                    vector.setNull(i);
                }
            }
            return;
        }
        SortedSetDocValues dv = state.sortedSet;
        for (int i = 0; i < count; i++) {
            if (dv.advanceExact(docIds[i])) {
                if (dv.docValueCount() > 1) {
                    throw new DocValuesLeafUnsupportedException(
                        DocValuesLeafUnsupportedException.Reason.MULTI_VALUED,
                        "field [" + vector.getName() + "] has " + dv.docValueCount() + " values at doc " + docIds[i]
                    );
                }
                writeUtf8(vector, i, dv.lookupOrd(dv.nextOrd()));
            } else {
                vector.setNull(i);
            }
        }
    }

    /**
     * Dictionary keyword decode: gather per-doc SEGMENT ordinals, then build a per-batch dictionary
     * of only the DISTINCT ords (sorted, so the dictionary is ordered like the term dict) and remap
     * each row to its dictionary index. One lookupOrd per distinct term per batch — the dictionary
     * A/B's cost model versus utf8's one-materialization-per-row.
     */
    private void decodeKeywordDictionary(int columnOrdinal, ColumnState state, int[] docIds, int count, IntVector indices)
        throws IOException {
        state.fallbackBatches++; // still per-doc ordValue at Lucene 10.5 (bulk ordValues is fork-only)
        long[] ords = scratch; // reuse the numeric scratch: segment ords fit in long
        for (int i = 0; i < count; i++) {
            int ord = -1;
            if (state.sorted != null) {
                SortedDocValues dv = state.sorted;
                if (dv.docID() <= docIds[i] && dv.advanceExact(docIds[i])) {
                    ord = dv.ordValue();
                }
            } else {
                SortedSetDocValues dv = state.sortedSet;
                if (dv.advanceExact(docIds[i])) {
                    if (dv.docValueCount() > 1) {
                        throw new DocValuesLeafUnsupportedException(
                            DocValuesLeafUnsupportedException.Reason.MULTI_VALUED,
                            "field [" + indices.getName() + "] has " + dv.docValueCount() + " values at doc " + docIds[i]
                        );
                    }
                    ord = (int) dv.nextOrd();
                }
            }
            ords[i] = ord;
        }
        // Distinct ords, ascending (-1 = null excluded).
        long[] sorted = new long[count];
        System.arraycopy(ords, 0, sorted, 0, count);
        java.util.Arrays.sort(sorted, 0, count);
        int distinct = 0;
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            if (sorted[i] >= 0 && sorted[i] != prev) {
                sorted[distinct++] = sorted[i];
                prev = sorted[i];
            }
        }
        // Materialize the per-batch dictionary vector (one lookupOrd per distinct term).
        VarCharVector dictVector = new VarCharVector("dict-" + indices.getName(), dictionaryAllocator);
        dictVector.allocateNew(distinct);
        for (int d = 0; d < distinct; d++) {
            BytesRef term = state.sorted != null ? state.sorted.lookupOrd((int) sorted[d]) : state.sortedSet.lookupOrd(sorted[d]);
            dictVector.setSafe(d, term.bytes, term.offset, term.length);
        }
        dictVector.setValueCount(distinct);
        if (currentDictionaries == null) {
            currentDictionaries = new org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider();
        }
        currentDictionaries.put(
            new org.apache.arrow.vector.dictionary.Dictionary(
                dictVector,
                new org.apache.arrow.vector.types.pojo.DictionaryEncoding(columnOrdinal, false, new ArrowType.Int(32, true))
            )
        );
        // Remap rows: segment ord -> dictionary index via binary search over the distinct set.
        for (int i = 0; i < count; i++) {
            if (ords[i] < 0) {
                indices.setNull(i);
            } else {
                int idx = java.util.Arrays.binarySearch(sorted, 0, distinct, ords[i]);
                assert idx >= 0 : "ord " + ords[i] + " missing from its own batch dictionary";
                indices.setSafe(i, idx);
            }
        }
    }

    private static void writeUtf8(FieldVector vector, int row, BytesRef term) {
        if (vector instanceof VarCharVector v) {
            v.setSafe(row, term.bytes, term.offset, term.length);
        } else if (vector instanceof ViewVarCharVector v) {
            v.setSafe(row, term.bytes, term.offset, term.length);
        } else {
            throw new DocValuesLeafUnsupportedException(
                DocValuesLeafUnsupportedException.Reason.UNSUPPORTED_FIELD_TYPE,
                "no utf8-decode into Arrow vector " + vector.getClass().getSimpleName() + " for [" + vector.getName() + "]"
            );
        }
    }

    @Override
    public List<ColumnDecodeStats> decodeStats() {
        List<ColumnDecodeStats> stats = new ArrayList<>(specs.size());
        for (int c = 0; c < specs.size(); c++) {
            ColumnState s = columns[c];
            stats.add(new ColumnDecodeStats(specs.get(c).arrowField().getName(), s.bulkBatches, s.fallbackBatches, s.nanos));
        }
        return stats;
    }

    @Override
    public void close() {
        releaseCurrentDictionaries();
        currentLeaf = null; // iterators belong to the reader lease; nothing else to free here
    }
}
