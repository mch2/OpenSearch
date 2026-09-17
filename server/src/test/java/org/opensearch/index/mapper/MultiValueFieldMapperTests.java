/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class MultiValueFieldMapperTests extends MapperServiceTestCase {

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    /**
     * Establishes {@code objectName} as an array-valued object, the way indexing does.
     *
     * <p>Two rounds, because the shape can only be recorded on a mapper that exists: the first parse
     * creates the object and its leaf, and only once that update is applied can the array-ness be
     * marked on it and published. Indexing gets the second round from the bulk retry that follows any
     * mapping update.
     */
    private void declareObjectArray(MapperService service, String objectName, String leafName) throws IOException {
        for (int round = 0; round < 2; round++) {
            ParsedDocument parsed = service.documentMapper().parse(source(b -> {
                b.startArray(objectName);
                b.startObject().field(leafName, "a").endObject();
                b.endArray();
            }), new CapturingDocumentInput());
            if (parsed.dynamicMappingsUpdate() != null) {
                merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));
            }
        }
        assertTrue(
            "precondition: " + objectName + " must be declared array-valued",
            service.documentMapper().objectMappers().get(objectName).multiValue()
        );
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
    public void testDynamicStringArrayBecomesMultiValuedText() throws IOException {
        // An unmapped string with no template maps to `text` on a pluggable index
        // (builderSupplierForText). The OpenTelemetry template maps attributes to keyword instead, but
        // either way the array shape is recorded.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.array("tags", "prod", "blue")), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper tags = (FieldMapper) service.documentMapper().mappers().getMapper("tags");
        assertEquals("text", tags.typeName());
        assertTrue(tags.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), containsString("\"multi_value\":true"));
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

    /**
     * The array shape belongs to the object, not to its leaves: {@code events} becomes
     * {@code LIST<STRUCT<start, stop>>}, so each leaf stays a scalar within its element. Marking the
     * leaves instead would store {@code STRUCT<start LIST, stop LIST>}, which cannot say which element
     * a value came from — see {@code DocumentParser#declareObjectArray}.
     *
     * <p>It takes two rounds, because the array-ness is recorded on the object's mapper and the first
     * parse is what creates it. Indexing gets the second round from the bulk retry.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testArrayOfObjectsDeclaresTheObjectArrayShaped() throws IOException {
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startArray("events");
            b.startObject().field("start", "a").endObject();
            b.startObject().field("stop", "b").endObject();
            b.endArray();
        };

        ParsedDocument first = service.documentMapper().parse(source(document), new CapturingDocumentInput());
        merge(service, dynamicMapping(first.dynamicMappingsUpdate()));
        assertFalse(
            "the parse that creates the object cannot mark a mapper that does not exist yet",
            service.documentMapper().objectMappers().get("events").multiValue()
        );

        ParsedDocument second = service.documentMapper().parse(source(document), new CapturingDocumentInput());
        merge(service, dynamicMapping(second.dynamicMappingsUpdate()));

        assertTrue("the object itself carries the array shape", service.documentMapper().objectMappers().get("events").multiValue());
        assertFalse(((FieldMapper) service.documentMapper().mappers().getMapper("events.start")).fieldType().isMultiValued());
        assertFalse(((FieldMapper) service.documentMapper().mappers().getMapper("events.stop")).fieldType().isMultiValued());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testArrayOfObjectsRepeatingALeafStaysElementScoped() throws IOException {
        // A leaf that appears in every element is still one value per element, not a multi-valued leaf.
        // Both values reach the document input — that is what keeps `[{"name":"start"},{"name":"stop"}]`
        // distinct from `[{"name":"start","other":"stop"}]` — but they are told apart by their element
        // ordinal rather than by the leaf being a list.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        declareObjectArray(service, "events", "name");

        CapturingDocumentInput input = new CapturingDocumentInput();
        service.documentMapper().parse(source(b -> {
            b.startArray("events");
            b.startObject().field("name", "start").endObject();
            b.startObject().field("name", "stop").endObject();
            b.endArray();
        }), input);

        assertEquals("both values belong to the one field", 2L, input.getFieldCount("events.name"));
        assertEquals("and the run is two elements long", Integer.valueOf(2), input.getObjectArrayCounts().get("events"));

        FieldMapper name = (FieldMapper) service.documentMapper().mappers().getMapper("events.name");
        assertFalse("the leaf stays scalar within its element", name.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), containsString("\"multi_value\":true"));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testArrayOfObjectsRepeatingANumericLeafStaysElementScoped() throws IOException {
        // Same as the text case, on a numeric leaf, so a per-type serialization gap cannot hide.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        service.documentMapper().parse(source(b -> {
            b.startArray("events");
            b.startObject().field("name", 1).endObject();
            b.startObject().field("name", 2).endObject();
            b.endArray();
        }), new CapturingDocumentInput());
        merge(service, dynamicMapping(service.documentMapper().parse(source(b -> {
            b.startArray("events");
            b.startObject().field("name", 1).endObject();
            b.endArray();
        }), new CapturingDocumentInput()).dynamicMappingsUpdate()));

        ParsedDocument parsed = service.documentMapper().parse(source(b -> {
            b.startArray("events");
            b.startObject().field("name", 3).endObject();
            b.endArray();
        }), new CapturingDocumentInput());

        assertThat(
            "the published mapping update must carry the amended shape",
            org.opensearch.core.common.Strings.toString(
                org.opensearch.core.xcontent.MediaTypeRegistry.JSON,
                parsed.dynamicMappingsUpdate()
            ),
            containsString("\"multi_value\":true")
        );
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));
        assertTrue(service.documentMapper().objectMappers().get("events").multiValue());
        assertFalse(((FieldMapper) service.documentMapper().mappers().getMapper("events.name")).fieldType().isMultiValued());
    }

    /**
     * An empty array of objects is a present, zero-length {@code LIST<STRUCT>}, so it stays distinct
     * from an absent one. The element loop never runs for {@code []}, so the length is reported
     * separately — see {@code DocumentParser#registerEmptyMultiValueArray}.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testEmptyArrayOfObjectsReportsAZeroLengthRun() throws IOException {
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        declareObjectArray(service, "events", "name");

        CapturingDocumentInput input = new CapturingDocumentInput();
        service.documentMapper().parse(source(b -> b.startArray("events").endArray()), input);

        assertEquals("an empty array of objects reports a run of zero", Integer.valueOf(0), input.getObjectArrayCounts().get("events"));
        assertTrue("and writes no leaf value", input.getCapturedFields().stream().noneMatch(e -> e.getKey().name().startsWith("events.")));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSecondValueForAFieldAnEarlierDocumentCreatedIsRejected() throws IOException {
        // The shape can only be amended while it is still provisional. Once an earlier document has
        // fixed the field as scalar, files exist that were written as scalar columns, so a later
        // multi-value document has to be refused rather than silently changing the column type.
        MapperService service = createMapperService(pluggableSettings(), mapping(b -> {}));
        ParsedDocument first = service.documentMapper().parse(source(b -> b.field("tags", "solo")), new CapturingDocumentInput());
        merge(service, dynamicMapping(first.dynamicMappingsUpdate()));

        MapperParsingException error = expectThrows(
            MapperParsingException.class,
            () -> service.documentMapper().parse(source(b -> b.array("tags", "one", "two")), new CapturingDocumentInput())
        );
        assertThat(
            org.opensearch.ExceptionsHelper.stackTrace(error),
            containsString("declare [multi_value: true] when creating the field mapping")
        );
    }

    public void testDetectionDoesNotApplyWithoutThePluggableDataFormat() throws IOException {
        // Lucene needs no declaration, so stamping one would change the mapping output of every existing
        // index. The parameter is still accepted when declared explicitly — see
        // testMultiValueIsAcceptedOnAnyIndex — but detection stays out of the way.
        MapperService service = createMapperService(getIndexSettings(), mapping(b -> {}));
        ParsedDocument parsed = service.documentMapper().parse(source(b -> b.array("codes", 1, 2)), new CapturingDocumentInput());
        merge(service, dynamicMapping(parsed.dynamicMappingsUpdate()));

        FieldMapper codes = (FieldMapper) service.documentMapper().mappers().getMapper("codes");
        assertFalse(codes.fieldType().isMultiValued());
        assertThat(service.documentMapper().mappingSource().string(), not(containsString("multi_value")));
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
