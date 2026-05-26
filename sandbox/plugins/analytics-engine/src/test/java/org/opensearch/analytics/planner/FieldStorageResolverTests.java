/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FieldStorageResolver} field storage resolution.
 */
public class FieldStorageResolverTests extends OpenSearchTestCase {

    public void testTextFieldGetsDocValuesInPrimaryFormat() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("name", Map.of("type", "text")));

        FieldStorageInfo info = resolver.resolve(List.of("name")).get(0);

        assertEquals("name", info.getFieldName());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of("lucene"), info.getIndexFormats());
    }

    public void testLongFieldGetsDocValuesInPrimaryFormat() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("age", Map.of("type", "long")));

        FieldStorageInfo info = resolver.resolve(List.of("age")).get(0);

        assertEquals("age", info.getFieldName());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of("lucene"), info.getIndexFormats());
    }

    public void testFieldWithAllStorageDisabledHasNoStorage() {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> newResolver("parquet", Map.of("name", Map.of("type", "text", "doc_values", false, "index", false)))
        );
        assertTrue("expected 'no storage' error, got: " + ex.getMessage(), ex.getMessage().contains("has no storage in any format"));
    }

    public void testMetadataFieldIdIsResolvable() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("name", Map.of("type", "text")));

        FieldStorageInfo info = resolver.resolve(List.of("_id")).get(0);

        assertEquals("_id", info.getFieldName());
        assertEquals("binary", info.getMappingType());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of(), info.getIndexFormats());
        assertFalse(info.isDerived());
    }

    public void testMetadataFieldRoutingIsResolvable() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("name", Map.of("type", "text")));

        FieldStorageInfo info = resolver.resolve(List.of("_routing")).get(0);

        assertEquals("_routing", info.getFieldName());
        assertEquals("keyword", info.getMappingType());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of(), info.getIndexFormats());
        assertFalse(info.isDerived());
    }

    public void testMetadataFieldsCoexistWithUserFields() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("title", Map.of("type", "keyword")));

        List<FieldStorageInfo> infos = resolver.resolve(List.of("title", "_id", "_routing"));

        assertEquals(3, infos.size());
        assertEquals("title", infos.get(0).getFieldName());
        assertEquals("_id", infos.get(1).getFieldName());
        assertEquals("_routing", infos.get(2).getFieldName());
    }

    public void testAliasFieldResolvesToTargetStorage() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of(
            "timestamp", Map.of("type", "date"),
            "@timestamp", Map.of("type", "alias", "path", "timestamp")
        ));

        FieldStorageInfo info = resolver.resolve(List.of("@timestamp")).get(0);

        assertEquals("timestamp", info.getFieldName());
        assertEquals("date", info.getMappingType());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
    }

    public void testAliasMapIsPopulated() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of(
            "timestamp", Map.of("type", "date"),
            "@timestamp", Map.of("type", "alias", "path", "timestamp")
        ));

        assertEquals(Map.of("@timestamp", "timestamp"), resolver.getAliasMap());
    }

    private static FieldStorageResolver newResolver(String primaryFormat, Map<String, Map<String, Object>> fieldMappings) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", primaryFormat)
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);

        return new FieldStorageResolver(indexMetadata);
    }
}
