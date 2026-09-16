/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 *
 * <p>An {@code object} field becomes a nullable Arrow {@code Struct}, so the Parquet writer stores
 * it as a group node rather than as unrelated dotted columns. The leaf column chunks are the same
 * either way — a struct's leaves get the same dotted paths ({@code city.name}), the same encodings,
 * and the same statistics — but the reader hands the object back assembled, and a document that
 * omitted the object reads as one null instead of as a row of coincidentally-null leaves.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     *
     * <p>A field whose mapper declares {@code multi_value: true}
     * ({@link MappedFieldType#isMultiValued()}) is emitted as a {@code LIST<element>} column;
     * every other field keeps its scalar column.
     * TODO - Get the mapping version while creating the schema
     *
     * @param mapperService the mapper service containing field mappings
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        List<Field> fields = new ArrayList<>();
        DocumentMapper documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        if (documentMapper != null) {
            MappingLookup mappers = documentMapper.mappers();
            Map<String, ObjectMapper> objects = mappers.objectMappers();
            // Leaves that belong to an object, keyed by the path of the object that owns them.
            // Sorted so a struct's children keep a stable order across shards and mapping updates:
            // Arrow struct equality is order-sensitive, and the merge compares types to decide
            // whether a batch can be passed through untouched.
            Map<String, Map<String, Field>> objectChildren = new HashMap<>();
            for (Mapper mapper : mappers) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField != null) {
                    boolean multiValue = isMultiValued(mapper);
                    if (multiValue && parquetField.supportsMultiValue() == false) {
                        throw new IllegalArgumentException(
                            "Field ["
                                + mapper.name()
                                + "] of type ["
                                + mapper.typeName()
                                + "] does not support [multi_value] storage in the parquet data format"
                        );
                    }
                    // A multi-field is not part of the object's shape — it hangs off a leaf, under
                    // `fields` rather than `properties` — so it stays a top-level column under its
                    // full dotted name. Putting it in the struct would give the struct a child the
                    // query layer never declares, and the analytics engine rejects a scan whose
                    // struct carries fields its schema does not.
                    Field arrowField = parquetField.toArrowField(mapper.name(), multiValue);
                    if (mappers.isMultiField(mapper.name())) {
                        fields.add(arrowField);
                    } else {
                        place(mapper.name(), arrowField, objects, objectChildren, fields);
                    }
                    handleNormalizedField(mapper, documentMapper, objects, objectChildren, fields, parquetField, multiValue);
                } else {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                }
            }
            fields.addAll(buildObjectFields(objects, objectChildren));
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    /**
     * Files a leaf either as a top-level column or as a child of the object that encloses it.
     *
     * <p>The child is named by the remainder of the path below that object, so its Parquet leaf path
     * spells out the full dotted name either way. That is what keeps every consumer of the dotted
     * convention working: per-field encoding settings, statistics lookups, and the analytics engine's
     * field-storage resolution all address {@code city.name} and still find it.
     *
     * <p>{@code leaf} arrives fully built, so a multi-valued field keeps its {@code LIST} wrapper and
     * element child on the way into the struct: only the field's own name is rewritten to the local
     * one, never its type.
     */
    private static void place(
        String fullName,
        Field leaf,
        Map<String, ObjectMapper> objects,
        Map<String, Map<String, Field>> objectChildren,
        List<Field> topLevel
    ) {
        String objectPath = enclosingObject(fullName, objects);
        if (objectPath == null) {
            topLevel.add(leaf);
            return;
        }
        String childName = fullName.substring(objectPath.length() + 1);
        objectChildren.computeIfAbsent(objectPath, path -> new TreeMap<>())
            .put(childName, new Field(childName, leaf.getFieldType(), leaf.getChildren()));
    }

    /**
     * Returns the path of the innermost {@code object} enclosing {@code fullName}, or null when the
     * field sits at the top level.
     *
     * <p>Resolved against the declared object paths rather than by splitting on dots, because a dot
     * in a field name does not imply an object: a multi-field ({@code city.name.keyword}) and the
     * derived-source companion of a keyword ({@code _ignored_source.city.name}) are both dotted, and
     * neither {@code city.name} nor {@code _ignored_source} is an object.
     *
     * <p>A {@code nested} object is not one either, as far as storage goes: it is an array of
     * sub-documents, which wants {@code LIST<STRUCT<..>>} and not a struct. Mapping creation rejects
     * {@code nested} on a pluggable-data-format index, so this is unreachable; treating it as absent
     * keeps the fallback a flat column rather than a silently wrong shape.
     */
    private static String enclosingObject(String fullName, Map<String, ObjectMapper> objects) {
        for (int dot = fullName.lastIndexOf('.'); dot > 0; dot = fullName.lastIndexOf('.', dot - 1)) {
            ObjectMapper object = objects.get(fullName.substring(0, dot));
            if (object != null && object.nested().isNested() == false) {
                return fullName.substring(0, dot);
            }
        }
        return null;
    }

    /**
     * Assembles one struct Field per object that owns at least one writable leaf.
     *
     * <p>Deepest object first, so a sub-object is already built by the time its parent is assembled
     * and can be filed as one of the parent's children. An object all of whose leaves are of a type
     * with no Parquet representation contributes no column at all — the same treatment such a leaf
     * gets on its own.
     */
    private static List<Field> buildObjectFields(Map<String, ObjectMapper> objects, Map<String, Map<String, Field>> objectChildren) {
        List<String> paths = new ArrayList<>(objects.keySet());
        paths.sort(Comparator.comparingInt(ArrowSchemaBuilder::depth).reversed());

        List<Field> topLevel = new ArrayList<>();
        for (String path : paths) {
            Map<String, Field> children = objectChildren.get(path);
            if (children == null || children.isEmpty()) {
                continue;
            }
            int lastDot = path.lastIndexOf('.');
            String localName = lastDot < 0 ? path : path.substring(lastDot + 1);
            Field struct = objects.get(path).multiValue()
                // An array of objects is one repeated group, so every leaf beneath it shares that
                // repetition level and stays tied to its element. Storing the leaves as independent
                // lists instead would lose which element each value came from.
                ? new Field(
                    localName,
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(
                        new Field(
                            ParquetField.LIST_ELEMENT_NAME,
                            FieldType.nullable(ArrowType.Struct.INSTANCE),
                            List.copyOf(children.values())
                        )
                    )
                )
                : new Field(localName, FieldType.nullable(ArrowType.Struct.INSTANCE), List.copyOf(children.values()));

            String parent = enclosingObject(path, objects);
            if (parent == null) {
                topLevel.add(struct);
            } else {
                objectChildren.computeIfAbsent(parent, key -> new TreeMap<>()).put(localName, struct);
            }
        }
        return topLevel;
    }

    private static int depth(String path) {
        int depth = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '.') {
                depth++;
            }
        }
        return depth;
    }

    private static void handleNormalizedField(
        Mapper mapper,
        DocumentMapper documentMapper,
        Map<String, ObjectMapper> objects,
        Map<String, Map<String, Field>> objectChildren,
        List<Field> topLevel,
        ParquetField parquetField,
        boolean multiValue
    ) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                // The raw-value companion holds the pre-normalization source for derived source, so
                // it must mirror the parent's cardinality or source reconstruction would lose values.
                place(rawValueField.name(), parquetField.toArrowField(rawValueField.name(), multiValue), objects, objectChildren, topLevel);
            }
        }
    }

    /** Reads the {@code multi_value} declaration from the mapper's field type. */
    private static boolean isMultiValued(Mapper mapper) {
        return mapper instanceof FieldMapper fieldMapper && fieldMapper.fieldType().isMultiValued();
    }

    private static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
