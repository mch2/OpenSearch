/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.delegation.DelegationTarget;
import org.opensearch.analytics.delegation.DelegationType;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.index.engine.DataFormatAwareEngine;

/**
 * Lucene analytics backend plugin.
 * <p>
 * Provides direct query execution via {@link LuceneSearchExecEngine} and
 * filter delegation via {@link LuceneFilterDelegationTarget}.
 */
public class LuceneSearchBackend implements AnalyticsSearchBackendPlugin {

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public SearchExecEngine searcher(ExecutionContext ctx, DataFormatAwareEngine.DataFormatAwareReader reader) {
        // TODO: resolve DataFormat properly instead of passing null
        DirectoryReader directoryReader = (DirectoryReader) reader.getReader(null);
        LuceneSearchContext luceneSearchContext = new LuceneSearchContext(directoryReader);
        LuceneSearchExecEngine luceneSearchExecEngine = new LuceneSearchExecEngine(luceneSearchContext);
        luceneSearchExecEngine.prepare(ctx);
        return luceneSearchExecEngine;
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }

    @Override
    public DelegationTarget getDelegationTarget(DelegationType type, SearchExecEngine engine) {
        if (type != DelegationType.FILTER) return null;
        return (DelegationTarget) engine;
    }
}
