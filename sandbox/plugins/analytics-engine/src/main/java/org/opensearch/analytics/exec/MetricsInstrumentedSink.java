/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;

/**
 * Wraps an {@link ExchangeSink} so every batch that flows through it
 * increments the given {@link StageMetrics}. Delegates all other sink
 * operations unchanged to the wrapped sink.
 *
 * <p>Row counting is authoritative: every row that reaches the delegate's
 * {@code feed} also increments {@code rowsProcessed}.
 *
 * <p>Byte counting is approximate for the {@link FragmentExecutionResponse}
 * path (rough estimate: rows × fields × 8 bytes per cell). As the engine
 * transitions to pure Arrow transport this approximation goes away.
 *
 * @opensearch.internal
 */
final class MetricsInstrumentedSink implements ExchangeSink {

    private final StageMetrics metrics;
    private final ExchangeSink delegate;

    MetricsInstrumentedSink(StageMetrics metrics, ExchangeSink delegate) {
        this.metrics = metrics;
        this.delegate = delegate;
    }

    @Override
    public void feed(FragmentExecutionResponse response) {
        if (response.hasMetadata() == false) {
            int rows = response.getRows().size();
            int fieldCount = response.getFieldNames() == null ? 0 : response.getFieldNames().size();
            long bytes = (long) rows * fieldCount * 8L;
            metrics.addRowsProcessed(rows);
            metrics.addBytesRead(bytes);
        }
        delegate.feed(response);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Iterable<Object[]> readResult() {
        return delegate.readResult();
    }

    @Override
    public long getRowCount() {
        return delegate.getRowCount();
    }

    @Override
    public Object getValueAt(String column, int rowIndex) {
        return delegate.getValueAt(column, rowIndex);
    }
}
