/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;

/**
 * Plugin providing Lucene as both a storage backend ({@link SearchBackEndPlugin})
 * and an analytics execution backend ({@link AnalyticsSearchBackendPlugin}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin extends Plugin
    implements
        SearchBackEndPlugin<DirectoryReader>,
        AnalyticsSearchBackendPlugin,
        DataFormatPlugin {

    public LuceneSearchEnginePlugin() {}

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    // ---- SearchBackEndPlugin (storage) ----

    @Override
    public EngineReaderManager<DirectoryReader> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneReaderManager(format);
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(LuceneDataFormat.INSTANCE);
    }

    // ---- DataFormatPlugin (format registration) ----

    @Override
    public DataFormat getDataFormat() {
        return LuceneDataFormat.INSTANCE;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings) {
        // Lucene indexing is handled by OpenSearch core, not this plugin.
        return null;
    }

    // ---- AnalyticsSearchBackendPlugin (capabilities + execution) ----

    @Override
    public java.util.Set<OperatorCapability> supportedOperators() {
        return java.util.Set.of(
            OperatorCapability.SCAN,
            OperatorCapability.FILTER,
            OperatorCapability.PROJECT,
            OperatorCapability.SORT,
            OperatorCapability.AGGREGATE,
            OperatorCapability.LOCAL_STAGE
        );
    }

    @Override
    public java.util.Set<AggregateCapability> aggregateCapabilities() {
        // Lucene can compute aggregates over doc values for numeric types
        java.util.Set<String> formats = java.util.Set.of(LuceneDataFormat.INSTANCE.name());
        return java.util.Set.of(
            AggregateCapability.simple(AggregateFunction.SUM, FieldType.LONG, formats),
            AggregateCapability.simple(AggregateFunction.SUM, FieldType.DOUBLE, formats),
            AggregateCapability.simple(AggregateFunction.SUM, FieldType.INTEGER, formats),
            AggregateCapability.simple(AggregateFunction.COUNT, FieldType.LONG, formats),
            AggregateCapability.simple(AggregateFunction.COUNT, FieldType.DOUBLE, formats),
            AggregateCapability.simple(AggregateFunction.COUNT, FieldType.INTEGER, formats),
            AggregateCapability.simple(AggregateFunction.COUNT, FieldType.KEYWORD, formats),
            AggregateCapability.simple(AggregateFunction.MIN, FieldType.LONG, formats),
            AggregateCapability.simple(AggregateFunction.MIN, FieldType.DOUBLE, formats),
            AggregateCapability.simple(AggregateFunction.MAX, FieldType.LONG, formats),
            AggregateCapability.simple(AggregateFunction.MAX, FieldType.DOUBLE, formats),
            AggregateCapability.simple(AggregateFunction.AVG, FieldType.LONG, formats),
            AggregateCapability.simple(AggregateFunction.AVG, FieldType.DOUBLE, formats)
        );
    }

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new LuceneFragmentConvertor();
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        try {
            DirectoryReader reader = ctx.getReader().getReader(LuceneDataFormat.INSTANCE, DirectoryReader.class);
            LuceneSearchContext luceneCtx = new LuceneSearchContext(ctx.getTask(), reader, new MatchAllDocsQuery());
            return new LuceneSearchExecEngine(luceneCtx);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Lucene search exec engine", e);
        }
    }
}
