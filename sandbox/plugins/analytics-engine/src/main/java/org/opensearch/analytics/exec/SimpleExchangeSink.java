/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link ExchangeSink} implementation that collects all rows into a list.
 * Captures field names from the first non-empty response.
 */
public class SimpleExchangeSink implements ExchangeSink {

    private final List<String> fieldNames = new ArrayList<>();
    private final List<Object[]> rows = new ArrayList<>();

    @Override
    public void feed(FragmentExecutionResponse response) {
        if (fieldNames.isEmpty() && response.getFieldNames().isEmpty() == false) {
            fieldNames.addAll(response.getFieldNames());
        }
        rows.addAll(response.getRows());
    }

    @Override
    public void close() {}

    @Override
    public Iterable<Object[]> readResult() {
        return rows;
    }

    @Override
    public long getRowCount() {
        return rows.size();
    }

    @Override
    public Object getValueAt(String column, int rowIndex) {
        int colIdx = fieldNames.indexOf(column);
        return (colIdx >= 0 && rowIndex < rows.size()) ? rows.get(rowIndex)[colIdx] : null;
    }
}
