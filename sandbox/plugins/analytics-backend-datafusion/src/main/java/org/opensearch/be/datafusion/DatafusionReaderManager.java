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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private static final String MOCK_FILE_NAME = "clickbench_hits_100.parquet";

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;
    private volatile boolean mockDataCopied = false;

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
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot)) {
            return readers.get(catalogSnapshot);
        }
        throw new IOException("No DataFusion reader available");
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
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
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot)) return;

        var files = catalogSnapshot.getSearchableFiles(dataFormat.name());

        // [indexing-mock] If no real parquet files from indexing pipeline, fall back to bundled
        // mock data so the real reader path is exercised during development.
        // Remove this block once parquet indexing is fully wired.
        if (files == null || files.isEmpty()) {
            ensureMockDataAvailable();
            DatafusionReader reader = new DatafusionReader(directoryPath, new String[] { MOCK_FILE_NAME });
            readers.put(catalogSnapshot, reader);
            return;
        }

        DatafusionReader reader = new DatafusionReader(directoryPath, files);
        readers.put(catalogSnapshot, reader);
    }

    /**
     * [indexing-mock] Copies the bundled mock parquet file into the shard's parquet directory so the
     * real DatafusionReader path is exercised during development before indexing is wired.
     * Remove this method once parquet indexing is fully wired.
     */
    private void ensureMockDataAvailable() throws IOException {
        if (mockDataCopied) return;
        Path dir = Path.of(directoryPath);
        Files.createDirectories(dir);
        Path target = dir.resolve(MOCK_FILE_NAME);
        if (Files.exists(target) == false) {
            try (InputStream in = getClass().getClassLoader().getResourceAsStream(MOCK_FILE_NAME)) {
                if (in == null) {
                    logger.warn("Mock parquet resource [{}] not found on classpath", MOCK_FILE_NAME);
                    return;
                }
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Copied mock parquet data to [{}]", target);
            }
        }
        mockDataCopied = true;
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }
}
