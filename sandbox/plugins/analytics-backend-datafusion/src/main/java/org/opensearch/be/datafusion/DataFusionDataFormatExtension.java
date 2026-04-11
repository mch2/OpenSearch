/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatExtension;

/**
 * Extension discovered by {@code ParquetDataFormatPlugin} via the
 * {@link org.opensearch.plugins.ExtensiblePlugin} mechanism.
 *
 * <p>Receives the fully-configured Parquet {@link DataFormat} (with field type capabilities)
 * and passes it to {@link DataFusionPlugin} so the backend can derive its query capabilities
 * from the write-side declaration.
 */
public class DataFusionDataFormatExtension implements DataFormatExtension {

    private final DataFusionPlugin plugin;

    /**
     * Single-arg constructor required by {@link org.opensearch.plugins.ExtensiblePlugin.ExtensionLoader}.
     * Receives the DataFusion plugin instance from the extending plugin.
     *
     * @param plugin the DataFusion plugin instance
     */
    public DataFusionDataFormatExtension(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void registerDataFormat(DataFormat format) {
        plugin.registerDataFormat(format);
    }
}
