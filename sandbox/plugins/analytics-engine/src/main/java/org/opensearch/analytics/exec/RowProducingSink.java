/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Terminal coordinator sink that eagerly materializes each incoming
 * {@link VectorSchemaRoot} into {@code Object[]} rows and releases the VSR
 * immediately. Accumulates only Java rows; no Arrow buffers are held past
 * {@link #feed} returning.
 *
 * <p>Releasing the VSR on each feed means the caller's buffers are returned
 * to their allocator synchronously — so the sink never needs to own or close
 * an allocator, and callers don't need to give it a dedicated child.
 *
 * <p>Not thread-safe: concurrent {@code feed} callers must synchronize
 * externally.
 *
 * <p>Implements both {@link ExchangeSink} and {@link ExchangeSource}:
 * producers write via {@code feed}, consumers read via {@code readResult}.
 */
public class RowProducingSink implements ExchangeSink, ExchangeSource {

    private final List<Object[]> rows = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();

    @Override
    public void feed(VectorSchemaRoot batch) {
        try {
            if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
                for (Field f : batch.getSchema().getFields()) {
                    fieldNames.add(f.getName());
                }
            }
            int colCount = batch.getFieldVectors().size();
            int rowCount = batch.getRowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = toJavaValue(batch.getVector(c), r);
                }
                rows.add(row);
            }
        } finally {
            batch.close();
        }
    }

    @Override
    public void close() {
        // No-op: rows hold only Java objects; the walker's terminal path calls
        // close() after readResult() has been handed to the listener, and the
        // listener may still be reading the rows reference when close() fires.
        // GC reclaims the rows list when the sink itself becomes unreachable.
    }

    @Override
    public Iterable<Object[]> readResult() {
        return rows;
    }

    @Override
    public long getRowCount() {
        return rows.size();
    }

    /**
     * Look up a cell value by column name and row index.
     *
     * @param column   the column name
     * @param rowIndex the zero-based row index
     * @return the cell value, or {@code null} if the column is unknown or the row index is out of range
     */
    public Object getValueAt(String column, int rowIndex) {
        int colIdx = fieldNames.indexOf(column);
        if (colIdx < 0) return null;
        if (rowIndex < 0 || rowIndex >= rows.size()) return null;
        return rows.get(rowIndex)[colIdx];
    }

    private static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return vector.getObject(index);
    }
}
