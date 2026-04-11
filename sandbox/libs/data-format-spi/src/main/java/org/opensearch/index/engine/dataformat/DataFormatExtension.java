/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

/**
 * Extension point for plugins that need to receive a {@link DataFormat} descriptor
 * from a data format plugin.
 *
 * <p>Any {@link org.opensearch.plugins.ExtensiblePlugin} that provides a data format
 * (e.g., parquet-data-format) can discover implementations of this interface via
 * {@link org.opensearch.plugins.ExtensiblePlugin.ExtensionLoader} and pass its
 * fully-configured {@link DataFormat} (with field type capabilities) so that
 * read-side backends can derive their query capabilities.
 *
 * <p>Extensions must have a single-arg constructor accepting their own plugin class,
 * as required by {@link org.opensearch.plugins.ExtensiblePlugin.ExtensionLoader}.
 */
public interface DataFormatExtension {

    /**
     * Called when a data format is available.
     *
     * @param format the data format with field type capabilities
     */
    void registerDataFormat(DataFormat format);
}
