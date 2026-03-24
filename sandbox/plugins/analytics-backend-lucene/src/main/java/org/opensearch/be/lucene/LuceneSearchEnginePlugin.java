/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SourceProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.ReaderManagerProvider;

import java.io.IOException;
import java.util.List;

/**
 * Plugin providing Lucene as an index filter or source provider.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin implements ReaderManagerProvider {

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }

    @Override
    public EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new LuceneReaderManager(format);
    }
}
