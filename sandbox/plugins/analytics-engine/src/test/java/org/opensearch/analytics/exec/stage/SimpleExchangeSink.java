/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSink;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal {@link ExchangeSink} for tests. Collects batches without conversion.
 */
class SimpleExchangeSink implements ExchangeSink {

    private final List<VectorSchemaRoot> batches = new ArrayList<>();

    @Override
    public void feed(VectorSchemaRoot batch) {
        batches.add(batch);
    }

    @Override
    public void close() {
        for (VectorSchemaRoot b : batches) {
            b.close();
        }
        batches.clear();
    }

    @Override
    public Iterable<Object[]> readResult() {
        return List.of();
    }

    @Override
    public long getRowCount() {
        long total = 0;
        for (VectorSchemaRoot b : batches) {
            total += b.getRowCount();
        }
        return total;
    }

    @Override
    public Object getValueAt(String column, int rowIndex) {
        return null;
    }
}
