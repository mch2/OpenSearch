/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of {@link LuceneFieldFactory} instances keyed by OpenSearch field type name.
 *
 * Provides a default registry pre-populated with factories for the standard full-text-searchable
 * types ({@code text}, {@code keyword}, {@code match_only_text}). Additional types can be
 * registered at runtime via {@link #register(String, LuceneFieldFactory)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneFieldFactoryRegistry {

    private static final FieldType ID_FIELD_TYPE = new FieldType();

    static {
        ID_FIELD_TYPE.setTokenized(false);
        ID_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        ID_FIELD_TYPE.setOmitNorms(true);
        ID_FIELD_TYPE.setStored(false);
        ID_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
        ID_FIELD_TYPE.freeze();
    }

    // ── Default factories ──
    private static final LuceneFieldFactory TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory KEYWORD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
        // When THIS format owns the field's columnar storage (lucene-primary analytics index),
        // also write SORTED_SET doc values — the classic mapper path adds this field separately
        // (KeywordFieldMapper.parseCreateField), so the pluggable path must too or the DV leaf
        // has nothing to decode. Skipped when parquet is primary (parquet claims COLUMNAR_STORAGE).
        Set<org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability> caps = ft.getCapabilityMap()
            .getOrDefault(LucenePlugin.DATA_FORMAT, Set.of());
        if (caps.contains(org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)) {
            doc.add(new SortedSetDocValuesField(ft.name(), new BytesRef(value.toString())));
        }
    };

    private static final LuceneFieldFactory MATCH_ONLY_TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory ID_FIELD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), new BytesRef((byte[]) value), ID_FIELD_TYPE));
    };

    private static final LuceneFieldFactory SEQ_NO_FIELD_FACTORY = (doc, ft, value, lft) -> {
        // do nothing for now since we don't want to index seq no indexing without soft deletes enabled.
    };

    // ── Doc-values factories (lucene-primary analytics indices) ──
    // Points + doc values, mirroring what the classic engine writes: points serve delegated
    // range/term predicates, doc values serve the analytics DV leaf's columnar decode.

    /** long/integer/short/byte/double/float — delegate to the mapper's NumberType encoding. */
    private static final LuceneFieldFactory NUMBER_FACTORY = (doc, ft, value, lft) -> {
        NumberFieldMapper.NumberType numberType = ((NumberFieldMapper.NumberFieldType) ft).numberType();
        numberType.createFields(ft.name(), (Number) value, true, true, false, false).forEach(doc::add);
    };

    /** date — the mapper hands an epoch-millis Long (resolution already applied). */
    private static final LuceneFieldFactory DATE_FACTORY = (doc, ft, value, lft) -> {
        long millis = (Long) value;
        doc.add(new LongPoint(ft.name(), millis));
        doc.add(new SortedNumericDocValuesField(ft.name(), millis));
    };

    /** boolean — encoded 1/0 in numeric doc values plus the classic "T"/"F" term. */
    private static final LuceneFieldFactory BOOLEAN_FACTORY = (doc, ft, value, lft) -> {
        boolean b = (Boolean) value;
        doc.add(new Field(ft.name(), b ? "T" : "F", BooleanFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new SortedNumericDocValuesField(ft.name(), b ? 1 : 0));
    };

    // ── Registry ──

    private final Map<String, LuceneFieldFactory> factories = new ConcurrentHashMap<>();

    /**
     * Creates a registry pre-populated with the default full-text-searchable field factories.
     */
    public LuceneFieldFactoryRegistry() {
        register(TextFieldMapper.CONTENT_TYPE, TEXT_FACTORY);
        register(KeywordFieldMapper.CONTENT_TYPE, KEYWORD_FACTORY);
        register(MatchOnlyTextFieldMapper.CONTENT_TYPE, MATCH_ONLY_TEXT_FACTORY);
        // Doc-values types (lucene-primary analytics indices)
        for (NumberFieldMapper.NumberType t : new NumberFieldMapper.NumberType[] {
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.INTEGER,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.DOUBLE,
            NumberFieldMapper.NumberType.FLOAT }) {
            register(t.typeName(), NUMBER_FACTORY);
        }
        register(DateFieldMapper.CONTENT_TYPE, DATE_FACTORY);
        register(BooleanFieldMapper.CONTENT_TYPE, BOOLEAN_FACTORY);
        registerMetaFields();
    }

    private void registerMetaFields() {
        register(IdFieldMapper.CONTENT_TYPE, ID_FIELD_FACTORY);
        register(SeqNoFieldMapper.CONTENT_TYPE, SEQ_NO_FIELD_FACTORY);
        register(SeqNoFieldMapper.PRIMARY_TERM_NAME, (d, ft, v, lft) -> d.add(new SortedNumericDocValuesField(ft.name(), (long) v)));
        register(SourceFieldMapper.CONTENT_TYPE, (d, ft, v, lft) -> d.add(new Field(ft.name(), (BytesRef) v, lft)));
        // pending routing and ignored field handling
    }

    /**
     * Registers a factory for the given field type name. Overwrites any existing registration.
     *
     * @param typeName the OpenSearch field type name (e.g., "text", "keyword")
     * @param factory  the factory that creates Lucene fields for this type
     */
    public void register(String typeName, LuceneFieldFactory factory) {
        factories.put(typeName, factory);
    }

    /**
     * Returns the factory for the given type name, or {@code null} if not registered.
     *
     * @param typeName the OpenSearch field type name
     * @return the factory, or null
     */
    public LuceneFieldFactory get(String typeName) {
        return factories.get(typeName);
    }

    /**
     * Returns the set of currently registered type names.
     *
     * @return unmodifiable set of supported type names
     */
    public Set<String> supportedTypes() {
        return Set.copyOf(factories.keySet());
    }
}
