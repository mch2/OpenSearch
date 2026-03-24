/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.index.engine.DataFormatAwareEngine;

/**
 * SPI adapter for the DataFusion analytics backend. Loaded by
 * {@code AnalyticsPlugin.loadExtensions()} via ServiceLoader with a
 * single-arg constructor taking the parent {@link DataFusionPlugin}.
 *
 * <p>Handles analytics planning concerns only (bridge, operator table, capabilities).
 * Per-shard search execution (readers, engines, filter providers) is handled by
 * {@link DataFusionPlugin} which implements {@code ReaderManagerProvider} directly.
 */
public class DataFusionSearchBackend implements AnalyticsSearchBackendPlugin {

    private final DataFusionService service;

    public DataFusionSearchBackend(DataFusionService service) {
        this.service = service;
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public SearchExecEngine searcher(ExecutionContext ctx, DataFormatAwareEngine.DataFormatAwareReader reader) {
        // TODO: resolve DataFormat properly instead of passing null
        DatafusionReader dfReader = (DatafusionReader) reader.getReader(null);
        DatafusionContext context = new DatafusionContext(dfReader, service.getNativeRuntime());
        DatafusionSearchExecEngine datafusionSearchExecEngine = new DatafusionSearchExecEngine(context);
        datafusionSearchExecEngine.prepare(ctx);
        return datafusionSearchExecEngine;
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return new DataFusionOperatorTable();
    }
}
