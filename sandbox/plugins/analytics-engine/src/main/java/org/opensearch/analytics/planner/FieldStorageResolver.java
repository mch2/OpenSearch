/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves per-field storage metadata from {@link IndexMetadata}.
 *
 * <p>Uses the index's {@code index.composite.primary_data_format} setting as the
 * doc value format for all fields that have doc values. Index formats are always
 * {@code "lucene"} for fields explicitly marked {@code index: true}. Stored fields
 * are {@code "lucene"} for fields explicitly marked {@code store: true}.
 *
 * <p>TODO: Replace with actual per-field format metadata once the indexing team adds
 * {@code doc_value_formats} / {@code index_formats} to MappingMetadata.
 *
 * @opensearch.internal
 */
public class FieldStorageResolver {

    // TODO: import from CompositeEnginePlugin.PRIMARY_DATA_FORMAT once composite-common
    // exposes it as a shared constant accessible to analytics-engine.
    static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";
    static final String SECONDARY_DATA_FORMATS_SETTING = "index.composite.secondary_data_formats";

    private static final String LUCENE_FORMAT = "lucene";

    private final Map<String, FieldStorageInfo> fieldStorage;
    private final Map<String, String> aliasToTarget;

    /**
     * Test constructor — explicit per-field storage, bypasses IndexMetadata inference.
     * Allows tests to declare hybrid fields (e.g. doc values in both parquet and lucene)
     * without needing actual IndexMetadata.
     *
     * TODO: remove once FieldStorageResolver is integrated with actual per-field format
     * metadata from MappingMetadata — tests should use real mappings at that point.
     */
    FieldStorageResolver(Map<String, FieldStorageInfo> fieldStorage) {
        this.fieldStorage = new HashMap<>(fieldStorage);
        this.aliasToTarget = Map.of();
    }

    @SuppressWarnings("unchecked")
    public FieldStorageResolver(IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String primaryFormat = indexMetadata.getSettings().get(PRIMARY_DATA_FORMAT_SETTING, LUCENE_FORMAT);
        // Lucene is index-viable only when it's the primary or in the secondary list.
        boolean luceneAvailable = LUCENE_FORMAT.equals(primaryFormat)
            || indexMetadata.getSettings().getAsList(SECONDARY_DATA_FORMATS_SETTING).contains(LUCENE_FORMAT);

        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            throw new IllegalStateException("No mapping found for index [" + indexName + "]");
        }
        Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
        if (properties == null) {
            throw new IllegalStateException("No properties in mapping for index [" + indexName + "]");
        }

        this.fieldStorage = new HashMap<>();
        this.aliasToTarget = new HashMap<>();
        populateFromProperties(properties, "", primaryFormat, luceneAvailable);
        populateMetadataFields(primaryFormat);
    }

    /**
     * Registers system metadata fields that the parquet data format plugin materializes
     * for every document but are not declared in the user mapping's {@code properties}.
     * The set mirrors {@code MetadataFieldPlugin.getParquetFields()}.
     */
    private void populateMetadataFields(String primaryFormat) {
        fieldStorage.put("_id", new FieldStorageInfo(
            "_id", "binary", FieldType.fromMappingType("binary"),
            List.of(primaryFormat), List.of(), List.of(), false
        ));
        fieldStorage.put("_routing", new FieldStorageInfo(
            "_routing", "keyword", FieldType.fromMappingType("keyword"),
            List.of(primaryFormat), List.of(), List.of(), false
        ));
    }

    @SuppressWarnings("unchecked")
    private void populateFromProperties(Map<String, Object> properties, String pathPrefix, String primaryFormat, boolean luceneAvailable) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = pathPrefix.isEmpty() ? entry.getKey() : pathPrefix + "." + entry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null) {
                Map<String, Object> nested = (Map<String, Object>) fieldProps.get("properties");
                if (nested != null) {
                    populateFromProperties(nested, fieldName, primaryFormat, luceneAvailable);
                    continue;
                }
                throw new IllegalStateException("Field [" + fieldName + "] has no type in mapping");
            }
            if ("alias".equals(fieldType)) {
                String targetPath = (String) fieldProps.get("path");
                if (targetPath != null) {
                    aliasToTarget.put(fieldName, targetPath);
                }
                continue;
            }
            this.fieldStorage.put(fieldName, resolveField(fieldName, fieldType, fieldProps, primaryFormat, luceneAvailable));
        }
    }

    /** Resolves storage info for the requested fields in order. Alias fields resolve to their target's storage. */
    public List<FieldStorageInfo> resolve(List<String> fieldNames) {
        List<FieldStorageInfo> result = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            FieldStorageInfo info = fieldStorage.get(fieldName);
            if (info == null) {
                String target = aliasToTarget.get(fieldName);
                if (target != null) {
                    info = fieldStorage.get(target);
                }
            }
            if (info == null) {
                throw new IllegalStateException("Field [" + fieldName + "] not found in field storage for index");
            }
            result.add(info);
        }
        return result;
    }

    /** Returns the alias→target map. Used by plan rewrites to redirect alias column references. */
    public Map<String, String> getAliasMap() {
        return aliasToTarget;
    }

    private static FieldStorageInfo resolveField(
        String fieldName,
        String fieldType,
        Map<String, Object> fieldProps,
        String primaryFormat,
        boolean luceneAvailable
    ) {
        // Doc values: present for all types unless explicitly disabled
        boolean hasDocValues = !Boolean.FALSE.equals(fieldProps.get("doc_values"));

        // Index: only when explicitly set to false in mapping - enabled by default.
        boolean isIndexed = !Boolean.FALSE.equals(fieldProps.get("index"));

        // Stored fields: only when explicitly set to true in mapping
        boolean isStored = Boolean.TRUE.equals(fieldProps.get("store"));

        List<String> docValueFormats = hasDocValues ? List.of(primaryFormat) : List.of();
        // Only declare Lucene formats when Lucene is actually an index data format.
        List<String> indexFormats = (isIndexed && luceneAvailable) ? List.of(LUCENE_FORMAT) : List.of();
        List<String> storedFieldFormats = (isStored && luceneAvailable) ? List.of(LUCENE_FORMAT) : List.of();

        if (docValueFormats.isEmpty() && indexFormats.isEmpty() && storedFieldFormats.isEmpty()) {
            throw new IllegalStateException("Field [" + fieldName + "] has no storage in any format");
        }

        return new FieldStorageInfo(
            fieldName,
            fieldType,
            FieldType.fromMappingType(fieldType),
            docValueFormats,
            indexFormats,
            storedFieldFormats,
            false
        );
    }
}
