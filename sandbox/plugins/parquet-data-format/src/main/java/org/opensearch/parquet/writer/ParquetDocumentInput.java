/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Document input for the Parquet data format.
 *
 * <p>Implements {@link DocumentInput} to collect field-value pairs incrementally during
 * document indexing. Fields are stored as {@link FieldValuePair} objects and later transferred
 * to Arrow vectors by {@link org.opensearch.parquet.vsr.VSRManager#addDocument(ParquetDocumentInput)}.
 *
 * <p>Calling {@link #close()} clears all collected fields and resets the row ID,
 * allowing the instance to be discarded cleanly after use.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    // Keyed by field name, not field-type identity: within a single document parse each logical
    // field (including the derived-source `_ignored_source.*` companion) has a unique name, while
    // identity would silently miss a match if the parser ever handed back a fresh wrapper per array
    // element — degrading a multi_value field to last-value-wins or bypassing the scalar duplicate
    // guard. Name keying makes accumulation robust to that.
    private final Map<String, FieldValuePair> seen = new HashMap<>();
    private final Map<String, Integer> objectArrayCounts = new HashMap<>();
    private long rowId = -1;
    private boolean isClosed = false;

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            // nothing to support on this format for this field.
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        FieldValuePair existing = seen.get(fieldType.name());
        if (existing == null) {
            // Fields declared `multi_value: true` in the mapping start out as a list of one so the
            // value shape reaching the VSR is the same whether the document had one value or several.
            // An empty List carries no value, so record nothing: the cell is then written null, which
            // is what an absent field writes. The parser already declines to register `"field": []`;
            // this is the second line of defence, so no caller can reintroduce a present-but-empty
            // cell that would read back as [] while Lucene reads the same document as null.
            if (fieldType.isMultiValued() && value instanceof List<?> list && list.isEmpty()) {
                return;
            }
            final FieldValuePair pair = fieldType.isMultiValued()
                ? FieldValuePair.multiValued(fieldType, value)
                : new FieldValuePair(fieldType, value);
            seen.put(fieldType.name(), pair);
            collectedFields.add(pair);
            return;
        }
        if (existing.isMultiValued() == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: ["
                    + fieldType.name()
                    + "] of type: ["
                    + fieldType.typeName()
                    + "]; declare [multi_value: true] when creating the field mapping"
            );
        }
        existing.addValue(value);
    }

    /**
     * Records a leaf of one element of an array of objects, keeping its element ordinal so the
     * writer can lay the object out as {@code LIST<STRUCT<..>>}. Repeated calls for the same field
     * fill in successive elements; an element that omitted the leaf leaves a null at its ordinal.
     */
    @Override
    public void addField(MappedFieldType fieldType, Object value, int elementOrdinal) {
        ensureOpen();
        if (elementOrdinal < 0) {
            addField(fieldType, value);
            return;
        }
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        FieldValuePair existing = seen.get(fieldType.name());
        if (existing == null) {
            FieldValuePair pair = FieldValuePair.elementIndexed(fieldType, value, elementOrdinal);
            seen.put(fieldType.name(), pair);
            collectedFields.add(pair);
            return;
        }
        if (existing.isElementIndexed() == false) {
            throw new MapperParsingException(
                "Field ["
                    + fieldType.name()
                    + "] was written as a plain value and then as an element of an array of objects; the two shapes cannot be mixed"
            );
        }
        existing.setElementValue(value, elementOrdinal);
    }

    @Override
    public void addObjectArray(String objectPath, int elementCount) {
        ensureOpen();
        if (elementCount == 0) {
            // An empty array of objects is an absent one: leaving the path unrecorded means the writer
            // never opens a run, so the LIST cell is null. Matches the scalar case above.
            return;
        }
        objectArrayCounts.merge(objectPath, elementCount, Math::max);
    }

    /**
     * Element counts for the objects this document carried as arrays, by dotted path.
     *
     * <p>Authoritative over anything the written values imply: an element that carried no field at all
     * still occupies a slot, so {@code [{},{}]} is a run of two. An empty array is never recorded — it
     * is written null, exactly as an absent field is.
     */
    public Map<String, Integer> getObjectArrayCounts() {
        return objectArrayCounts;
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        if (!isClosed) {
            assert rowId >= 0 : "Row ID must be set before calling getFinalInput";
            // assertions for parquet primary
            // TODO: once parquet is supported in secondary mode, this assertion would change
            assert getFieldCount(IdFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.NAME) == 1;
            assert getFieldCount(VersionFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.PRIMARY_TERM_NAME) == 1;
        }
        return collectedFields;
    }

    @Override
    public long getFieldCount(String fieldName) {
        // Counts values, not entries: a multi-valued field is one entry holding N values, and
        // callers (single-value assertions below, the data-stream @timestamp check) mean values.
        //
        // O(1) via the name index: addField routes every value for a name into the single pair
        // registered under that name in `seen`, so `seen` and `collectedFields` always hold the
        // same pairs and the lookup is exact. This is on the per-value hot path — the mapper calls
        // it before every scalar keyword value to reject a second value without scanning all
        // collected fields, so a linear scan of collectedFields here would make document parsing
        // quadratic in the field count.
        FieldValuePair pair = seen.get(fieldName);
        return pair == null ? 0 : pair.valueCount();
    }

    @Override
    public void close() {
        isClosed = true;
        collectedFields.clear();
        seen.clear();
        rowId = -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Cannot add more fields to a frozen document input");
        }
    }

    /**
     * Returns the row ID assigned to this document.
     *
     * @return the row ID, or -1 if not set
     */
    public long getRowId() {
        return rowId;
    }
}
