/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages {@link DatafusionReader} instances per shard.
 * <p>
 * On refresh, a new reader is created from the updated catalog snapshot.
 * File lifecycle events (add/delete) are delegated to the node-level
 * {@link DataFusionService} for cache management.
 *
 * <p>If {@link #getReader(CatalogSnapshot)} is called for a catalog snapshot that
 * {@link #afterRefresh(boolean, CatalogSnapshot)} has not yet populated (for
 * example: the first query against a freshly-created shard arrives before the
 * background refresh hook has fired), the reader is built lazily from the
 * snapshot's current searchable files and cached. This closes the gap where a
 * valid snapshot would otherwise produce a gratuitous {@code "No DataFusion
 * reader available"} I/O error.
 *
 * <p>If the snapshot carries zero searchable files (ingest buffered but not yet
 * flushed to parquet), the lazily-built reader will see zero files and the
 * query will return empty results. That is a correctness signal the caller must
 * address — typically by issuing a refresh before querying — not a failure mode
 * this manager should mask.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;

    /**
     * Creates a reader manager.
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     */
    public DatafusionReaderManager(DataFormat dataFormat, ShardPath shardPath, DataFusionService dataFusionService) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
    }

    @Override
    public synchronized DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return readers.get(catalogSnapshot);
        }
        DatafusionReader reader = buildReader(catalogSnapshot);
        readers.put(catalogSnapshot, reader);
        logger.debug(
            "DatafusionReader lazily built for snapshot [generation={}] on [{}]",
            catalogSnapshot.getGeneration(),
            directoryPath
        );
        return reader;
    }

    /**
     * Package-private factory hook. Production code constructs a native-backed
     * {@link DatafusionReader}; tests override this to stub native allocation.
     */
    DatafusionReader buildReader(CatalogSnapshot catalogSnapshot) {
        return new DatafusionReader(directoryPath, catalogSnapshot.getSearchableFiles(dataFormat.name()));
    }

    @Override
    public synchronized void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot);
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesAdded(toAbsolutePaths(files));
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public synchronized void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot)) return;
        DatafusionReader reader = buildReader(catalogSnapshot);
        readers.put(catalogSnapshot, reader);
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }

    @Override
    public synchronized void close() throws IOException {
        for (DatafusionReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
    }
}
