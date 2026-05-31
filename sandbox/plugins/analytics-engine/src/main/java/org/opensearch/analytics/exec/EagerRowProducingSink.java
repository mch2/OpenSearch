/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exchange sink that eagerly materializes Arrow batches into heap-resident rows
 * on {@link #feed}, closing each batch immediately to release Arrow pool memory.
 *
 * <p>Used at the root output position where the final consumer is row-oriented.
 * Each batch is converted to {@code Object[]} rows and the Arrow buffer is freed
 * on arrival — no accumulation in the Arrow allocator.
 *
 * <p>For stages that need VSR-based buffering (e.g. LateMaterialization's K-row buffer),
 * use {@link RowProducingSink} instead.
 */
public class EagerRowProducingSink implements ExchangeSink, ExchangeSource {

    private static final int MAX_ROWS = 10_000;

    private final List<Object[]> rows = new ArrayList<>();
    private final List<String> fieldNames = new ArrayList<>();
    private long totalRows;
    private boolean truncated;

    public EagerRowProducingSink() {}

    @Override
    public synchronized void feed(VectorSchemaRoot batch) {
        try {
            if (fieldNames.isEmpty() && batch.getSchema().getFields().isEmpty() == false) {
                for (Field f : batch.getSchema().getFields()) {
                    fieldNames.add(f.getName());
                }
            }
            if (rows.size() >= MAX_ROWS) {
                truncated = true;
                totalRows += batch.getRowCount();
                return;
            }
            int rowCount = batch.getRowCount();
            List<FieldVector> vectors = batch.getFieldVectors();
            int colCount = vectors.size();
            for (int r = 0; r < rowCount; r++) {
                if (rows.size() >= MAX_ROWS) {
                    truncated = true;
                    totalRows += (rowCount - r);
                    break;
                }
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = ArrowValues.toJavaValue(vectors.get(c), r);
                }
                rows.add(row);
            }
            totalRows += rowCount;
        } finally {
            batch.close();
        }
    }

    @Override
    public synchronized void close() {
        rows.clear();
    }

    /**
     * Direct access to materialized rows.
     */
    public synchronized Iterable<Object[]> readRows() {
        return new ArrayList<>(rows);
    }

    public synchronized List<String> getFieldNames() {
        return Collections.unmodifiableList(fieldNames);
    }

    @Override
    public synchronized Iterable<VectorSchemaRoot> readResult() {
        throw new UnsupportedOperationException(
            "EagerRowProducingSink materializes rows on feed(); use readRows() instead"
        );
    }

    @Override
    public synchronized long getRowCount() {
        return totalRows;
    }
}
