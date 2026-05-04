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

    /**
     * OpenSearch's {@link MappingMetadata#sourceAsMap()} strips {@code "type": "object"} when
     * {@code properties} is present. Parent object containers must be recognized by their
     * {@code properties} key and their children flattened to dotted paths.
     */
    public void testNestedObjectWithImplicitTypeFlattensToDotPath() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of("agent", Map.of("properties", Map.of("name", Map.of("type", "keyword"))))
        );

        FieldStorageInfo info = resolver.resolve(List.of("agent.name")).get(0);

        assertEquals("agent.name", info.getFieldName());
        assertEquals("keyword", info.getMappingType());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of("lucene"), info.getIndexFormats());
    }

    /** Explicit {@code "type": "object"} containers should also recurse into their properties. */
    public void testNestedObjectWithExplicitTypeFlattensToDotPath() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of("agent", Map.of("type", "object", "properties", Map.of("name", Map.of("type", "keyword"))))
        );

        FieldStorageInfo info = resolver.resolve(List.of("agent.name")).get(0);
        assertEquals("agent.name", info.getFieldName());
        assertEquals("keyword", info.getMappingType());
    }

    /**
     * {@code "type": "nested"} containers with a properties map should also flatten to dotted
     * leaves — the parquet backend stores nested-child fields as flat dotted-name columns,
     * not as Arrow struct types. Nested-query semantics (child-doc-as-row) are out of scope;
     * for scalar access, recursing on {@code nested} the same as {@code object} is correct.
     */
    public void testNestedTypeContainerFlattensToDotPath() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of("events", Map.of("type", "nested", "properties", Map.of("name", Map.of("type", "keyword"))))
        );

        FieldStorageInfo info = resolver.resolve(List.of("events.name")).get(0);
        assertEquals("events.name", info.getFieldName());
        assertEquals("keyword", info.getMappingType());
    }

    /** Deep nesting must flatten all levels, e.g. resource.attributes.telemetry.sdk.version. */
    public void testDeeplyNestedObjectFlattens() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of(
                "resource",
                Map.of(
                    "properties",
                    Map.of(
                        "attributes",
                        Map.of(
                            "properties",
                            Map.of(
                                "telemetry",
                                Map.of("properties", Map.of("sdk", Map.of("properties", Map.of("version", Map.of("type", "keyword")))))
                            )
                        )
                    )
                )
            )
        );

        FieldStorageInfo info = resolver.resolve(List.of("resource.attributes.telemetry.sdk.version")).get(0);
        assertEquals("resource.attributes.telemetry.sdk.version", info.getFieldName());
        assertEquals("keyword", info.getMappingType());
    }

    /** A leaf with multi-fields ({@code fields: { keyword: ... }}) should resolve to its primary type. */
    public void testLeafWithMultiFieldsUsesPrimaryType() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of("message", Map.of("type", "text", "fields", Map.of("keyword", Map.of("type", "keyword"))))
        );

        FieldStorageInfo info = resolver.resolve(List.of("message")).get(0);
        assertEquals("text", info.getMappingType());
    }

    /** An object container marked {@code enabled: false} should be skipped entirely (no fields resolved). */
    public void testDisabledObjectSubtreeIsSkipped() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of(
                "kept",
                Map.of("type", "keyword"),
                "metadata",
                Map.of("enabled", false, "properties", Map.of("ignored", Map.of("type", "keyword")))
            )
        );

        // Present field resolves.
        assertEquals("keyword", resolver.resolve(List.of("kept")).get(0).getMappingType());
        // Disabled subtree's leaves are absent.
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> resolver.resolve(List.of("metadata.ignored")));
        assertTrue(ex.getMessage().contains("not found in field storage"));
    }

    /**
     * Mixed flat + nested mapping (the common real-world shape for big5 / OTel-style indices)
     * should expose both the flat leaf and the nested leaf under their dotted path.
     */
    public void testMixedFlatAndNestedMappingResolvesBoth() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of(
                "@timestamp",
                Map.of("type", "date"),
                "agent",
                Map.of("properties", Map.of("name", Map.of("type", "keyword"), "id", Map.of("type", "keyword")))
            )
        );

        assertEquals("date", resolver.resolve(List.of("@timestamp")).get(0).getMappingType());
        assertEquals("keyword", resolver.resolve(List.of("agent.name")).get(0).getMappingType());
        assertEquals("keyword", resolver.resolve(List.of("agent.id")).get(0).getMappingType());
    }

    private static FieldStorageResolver newResolver(String primaryFormat, Map<String, Map<String, Object>> fieldMappings) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.composite.primary_data_format", primaryFormat).build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);

        return new FieldStorageResolver(indexMetadata);
    }
}
