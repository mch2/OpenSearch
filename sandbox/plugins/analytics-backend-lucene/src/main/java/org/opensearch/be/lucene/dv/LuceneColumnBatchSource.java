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

    private final List<DvColumnSpec> specs;
    private final long[] scratch;
    private final int[] docsScratch;

    // Per-column iterator state, valid for the CURRENT segment only.
    private LeafReaderContext currentLeaf;
    private final ColumnState[] columns;

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
        this.specs = specs;
        this.scratch = new long[batchSize];
        this.docsScratch = new int[batchSize];
        this.columns = new ColumnState[specs.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = new ColumnState();
        }
    }

    @Override
    public void decodeBatch(LeafReaderContext leaf, int[] docIds, int count, VectorSchemaRoot out) throws IOException {
        assert assertAscending(docIds, count);
        if (leaf != currentLeaf) {
            openSegment(leaf);
        }
        for (int c = 0; c < specs.size(); c++) {
            DvColumnSpec spec = specs.get(c);
            ColumnState state = columns[c];
            FieldVector vector = out.getVector(c);
            long start = System.nanoTime();
            if (spec.kind() == DvColumnSpec.DecodeKind.KEYWORD_ORD) {
                decodeKeyword(state, docIds, count, vector);
            } else {
                decodeNumeric(spec.kind(), state, docIds, count, vector);
            }
            state.nanos += System.nanoTime() - start;
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
        currentLeaf = null; // iterators belong to the reader lease; nothing to free here
    }
}
