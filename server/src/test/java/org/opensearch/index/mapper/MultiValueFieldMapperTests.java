/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class MultiValueFieldMapperTests extends MapperServiceTestCase {

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    private DocumentMapper keywordMapper() throws IOException {
        return keywordMapper(null);
    }

    private DocumentMapper keywordMapper(Boolean multiValue) throws IOException {
        return createDocumentMapper(pluggableSettings(), mapping(b -> {
            b.startObject("field").field("type", "keyword");
            if (multiValue != null) {
                b.field("multi_value", multiValue);
            }
            b.endObject();
        }));
    }

    public void testMultiValueIsAcceptedOnAnyIndex() throws IOException {
        // The declaration is a fact about the data, not about the storage format, so it is accepted
        // without the pluggable data format. Lucene ignores it — a posting list holds any number of
        // terms — but recording it lets a caller know the field's shape.
        MapperService service = createMapperService(
            getIndexSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );
        FieldMapper field = (FieldMapper) service.documentMapper().mappers().getMapper("field");
        assertTrue(field.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), containsString("\"multi_value\":true"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterDefaultsToScalarAndIsNotSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertFalse(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), not(containsString("multi_value")));

        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.field("field", "prod")), input);
        assertEquals(1L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOmittedParameterRejectsMultipleValuesWithoutMappingUpdate() throws IOException {
        DocumentMapper mapper = keywordMapper();
        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field").value("prod").value("error").endArray()), new CapturingDocumentInput())
        );

        assertThat(
            org.opensearch.ExceptionsHelper.stackTrace(error),
            containsString("declare [multi_value: true] when creating the field mapping")
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testScalarFieldIgnoresEmptyArray() throws IOException {
        DocumentMapper mapper = keywordMapper();
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        assertEquals(0L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitFalseIsScalarAndSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper(false);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertFalse(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":false"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTrueIsMultiValuedAndSerialized() throws IOException {
        DocumentMapper mapper = keywordMapper(true);
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");

        assertTrue(fieldMapper.fieldType().isMultiValued());
        assertThat(mapper.mappingSource().string(), containsString("\"multi_value\":true"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTrueAcceptsScalarAndArrayWithoutMappingUpdates() throws IOException {
        DocumentMapper mapper = keywordMapper(true);

        CapturingDocumentInput scalarInput = new CapturingDocumentInput();
        ParsedDocument scalar = mapper.parse(source(b -> b.field("field", "prod")), scalarInput);
        assertEquals(1L, scalarInput.getFieldCount("field"));
        assertNull(scalar.dynamicMappingsUpdate());

        CapturingDocumentInput arrayInput = new CapturingDocumentInput();
        ParsedDocument array = mapper.parse(source(b -> b.array("field", "prod", "error")), arrayInput);
        assertEquals(2L, arrayInput.getFieldCount("field"));
        assertNull(array.dynamicMappingsUpdate());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitTruePreservesEmptyArray() throws IOException {
        DocumentMapper mapper = keywordMapper(true);
        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.startArray("field").endArray()), input);

        Object emptyValue = input.getCapturedFields()
            .stream()
            .filter(entry -> entry.getKey().name().equals("field"))
            .map(java.util.Map.Entry::getValue)
            .findFirst()
            .orElseThrow();
        assertEquals(List.of(), emptyValue);
        assertNull(parsed.dynamicMappingsUpdate());
    }

    // ── dynamic detection ───────────────────────────────────────────────────────────────────────
    //
    // A field's shape is fixed when the field is created, so a dynamically created field has to be
    // declared array-shaped from the document that creates it. This is what makes wildcard mappings
    // workable: an OpenTelemetry template routes `attributes.*` through a dynamic template and cannot
    // enumerate which attribute keys hold arrays.

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicStringArrayBecomesTextAndStaysScalar() throws IOException {
        // An unmapped string with no template maps to `text` on a pluggable index
        // (builderSupplierForText), and text has no parquet list writer, so detection cannot make it
        // an array. Reaching an array-shaped string field requires a template that maps to keyword —
        // which is what the OpenTelemetry template does. Pinned so a change to the dynamic string
        // default is a deliberate decision rather than a surprise.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.array("tags", "prod", "blue")), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper tags = (FieldMapper) service.documentMapper().mappers().getMapper("tags");
        assertEquals("text", tags.typeName());
        assertFalse(tags.fieldType().isMultiValued());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDynamicFieldFromScalarStaysScalar() throws IOException {
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.field("count", 1)), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper count = (FieldMapper) service.documentMapper().mappers().getMapper("count");
        assertFalse(count.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), not(containsString("multi_value")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSingleElementArrayIsStillDeclaredMultiValued() throws IOException {
        // The producer contract: emit the array form on the first document even when it holds one
        // value, and the field is array-shaped from then on.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.array("codes", 7)), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper codes = (FieldMapper) service.documentMapper().mappers().getMapper("codes");
        assertTrue(codes.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), containsString("\"multi_value\":true"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDetectionAppliesThroughADynamicTemplate() throws IOException {
        // The OpenTelemetry template shape: a path_match wildcard over attributes.* that maps strings
        // to keyword. Detection has to add array-ness on top of whatever the template specifies,
        // because the template cannot name the individual attribute keys.
        MapperService service = createMapperService(pluggableSettings(), topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("string_attributes");
            b.field("path_match", "attributes.*");
            b.field("match_mapping_type", "string");
            b.startObject("mapping").field("type", "keyword").field("ignore_above", 256).endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }));

        ParsedDocument parsed = service.documentMapper().parse(source(b -> {
            b.startObject("attributes");
            b.array("tags", "prod", "blue");
            b.field("host", "node-1");
            b.endObject();
        }), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper arrayAttribute = (FieldMapper) service.documentMapper().mappers().getMapper("attributes.tags");
        FieldMapper scalarAttribute = (FieldMapper) service.documentMapper().mappers().getMapper("attributes.host");

        assertTrue("template-mapped attribute arriving as an array is multi-valued", arrayAttribute.fieldType().isMultiValued());
        assertFalse("template-mapped attribute arriving as a scalar stays scalar", scalarAttribute.fieldType().isMultiValued());
        assertEquals("keyword", arrayAttribute.typeName());
        assertThat(
            "the template's own settings still apply to the array field",
            service.documentMapper().mappingSource().string(),
            containsString("\"ignore_above\":256")
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDetectionCoversNonStringElementTypes() throws IOException {
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> {
            b.array("codes", 1, 2);
            b.array("ratios", 1.5, 2.5);
            b.array("flags", true, false);
        }), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        for (String field : List.of("codes", "ratios", "flags")) {
            FieldMapper mapper = (FieldMapper) service.documentMapper().mappers().getMapper(field);
            assertTrue(field + " should be multi-valued", mapper.fieldType().isMultiValued());
        }
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testArrayOfObjectsLeavesLeafFieldsScalar() throws IOException {
        // The array wraps objects, so the leaves inside them each hold one value. Array-ness must not
        // leak from the enclosing array down to those leaves.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> {
            b.startArray("events");
            b.startObject().field("name", "start").endObject();
            b.startObject().field("name", "stop").endObject();
            b.endArray();
        }), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper name = (FieldMapper) service.documentMapper().mappers().getMapper("events.name");
        assertFalse("a leaf inside an array of objects holds one value per object", name.fieldType().isMultiValued());
    }

    public void testDetectionAlsoAppliesWithoutThePluggableDataFormat() throws IOException {
        // Array-ness is recorded regardless of storage format, so a caller can tell a multi-valued
        // field from a single-valued one on a Lucene-backed index too.
        MapperService service = createMapperService(getIndexSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.array("codes", 1, 2)), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper codes = (FieldMapper) service.documentMapper().mappers().getMapper("codes");
        assertTrue(codes.fieldType().isMultiValued());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMultiValueCannotChangeAfterFieldCreation() throws IOException {
        MapperService scalarService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        IllegalArgumentException scalarToList = expectThrows(
            IllegalArgumentException.class,
            () -> merge(scalarService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject()))
        );
        assertThat(scalarToList.getMessage(), containsString("Cannot update parameter [multi_value] from [false] to [true]"));

        MapperService listService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );
        merge(listService, mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertTrue(listService.fieldType("field").isMultiValued());

        IllegalArgumentException listToScalar = expectThrows(
            IllegalArgumentException.class,
            () -> merge(listService, mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", false).endObject()))
        );
        assertThat(listToScalar.getMessage(), containsString("Cannot update parameter [multi_value] from [true] to [false]"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIndexSortFieldAcceptsArrayWhenDeclaredMultiValue() throws IOException {
        Settings settings = Settings.builder()
            .put(pluggableSettings())
            .putList("index.sort.field", "field")
            .putList("index.sort.order", "asc")
            .build();
        DocumentMapper mapper = createDocumentMapper(
            settings,
            mapping(b -> b.startObject("field").field("type", "keyword").field("multi_value", true).endObject())
        );

        CapturingDocumentInput input = new CapturingDocumentInput();
        ParsedDocument parsed = mapper.parse(source(b -> b.array("field", "z", "a")), input);
        assertEquals(2L, input.getFieldCount("field"));
        assertNull(parsed.dynamicMappingsUpdate());
    }
}
