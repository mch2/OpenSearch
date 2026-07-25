/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * <p>
 * Equality is based on the format {@link #name()} — there should be one {@code DataFormat} instance
 * per unique name. This allows {@code DataFormat} to be used safely as a {@link java.util.Map} key.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DataFormat {
    /**
     * Returns the unique name of this data format.
     *
     * @return the data format name
     */
    public abstract String name();

    /**
     * Returns the priority of this data format. Higher priority formats are preferred
     * when multiple formats can handle the same field type.
     *
     * @return the priority value
     */
    public abstract long priority();

    /**
     * Returns the set of field type capabilities supported by this data format.
     *
     * @return the supported field type capabilities
     */
    public abstract Set<FieldTypeCapabilities> supportedFields();

    /**
     * Adapts a live shard {@link OpenSearchDirectoryReader} into this format's analytics reader type
     * (e.g. the Lucene backend's {@code LuceneReader}), returned as an opaque {@link Object} so the
     * server does not compile-depend on the plugin type. Used by the PLAIN-index analytics reader
     * bridge in {@code IndexShard.getReaderProvider()}: a normal index's Lucene segments are scanned
     * directly by the analytics doc-values leaf, with the analytics backend casting this object back
     * to its concrete reader via {@code IndexReaderProvider.Reader.getReader(format, type)}.
     *
     * <p>Default returns {@code null} — a format that cannot back analytics scans (or is not the
     * Lucene format) contributes no entry to the reader map. Only the Lucene {@code DataFormat}
     * overrides this. Constructing the reader here (plugin-side) keeps {@code LuceneReader} out of
     * the server's compile classpath.
     *
     * @param directoryReader the shard's point-in-time reader (already carrying the shard's soft-delete
     *                        and security wrappers, since the bridge acquires it via
     *                        {@code IndexShard.acquireSearcher})
     * @return the format-specific analytics reader, or {@code null} if unsupported
     */
    @ExperimentalApi
    public Object adaptDirectoryReaderForAnalytics(OpenSearchDirectoryReader directoryReader) {
        return null;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DataFormat == false) return false;
        return Objects.equals(name(), ((DataFormat) o).name());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }
}
