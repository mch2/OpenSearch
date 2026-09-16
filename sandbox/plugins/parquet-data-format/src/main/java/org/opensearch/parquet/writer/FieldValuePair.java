/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.mapper.MappedFieldType;

import java.util.ArrayList;
import java.util.List;

/**
 * Pair of an OpenSearch {@link MappedFieldType} and the value(s) parsed for it.
 *
 * <p>Represents a single field entry collected by {@link ParquetDocumentInput} during
 * document indexing. The field type is used to resolve the corresponding Arrow vector
 * type via {@link org.opensearch.parquet.fields.ArrowFieldRegistry}, and the value is
 * written into that vector during document transfer to the VSR.
 *
 * <p>Scalar pairs are immutable and hold exactly one value. Pairs created via
 * {@link #multiValued} back a Parquet LIST column and are <em>mutable</em>: {@link #addValue}
 * appends as the document parser reports each array element, so {@link #getValue()} returns a
 * {@code List} for those — including a single-element list when the document supplied one value, or
 * an empty list for an explicit empty array via {@link #emptyMultiValued}.
 *
 * <p>Because multi-valued pairs grow during parsing, a reference must not be read until the
 * document is finalized. The sole consumer, {@code VSRManager#addDocument}, reads only through
 * {@link ParquetDocumentInput#getFinalInput()} after the whole document has been parsed, so no
 * caller observes a partially populated list. Do not cache or hand a pair across threads before
 * then.
 *
 * <p>The field type must not be null (enforced by constructor); values may be null
 * for nullable fields.
 */
public class FieldValuePair {

    private final MappedFieldType fieldType;
    private Object value;
    private List<Object> values;
    private boolean elementIndexed;

    /**
     * Creates a single-valued FieldValuePair.
     *
     * @param fieldType the mapped field type
     * @param value the parsed field value
     */
    public FieldValuePair(MappedFieldType fieldType, Object value) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType cannot be null");
        }
        this.fieldType = fieldType;
        this.value = value;
        this.values = null;
    }

    private FieldValuePair(MappedFieldType fieldType, List<Object> values) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType cannot be null");
        }
        this.fieldType = fieldType;
        this.value = null;
        this.values = values;
    }

    /**
     * Creates a multi-valued FieldValuePair seeded with its first value. Further values are
     * appended via {@link #addValue}, preserving document order and any duplicates.
     *
     * @param fieldType the mapped field type
     * @param firstValue the first parsed value
     * @return a multi-valued pair
     */
    public static FieldValuePair multiValued(MappedFieldType fieldType, Object firstValue) {
        List<Object> values = new ArrayList<>(1);
        values.add(firstValue);
        return new FieldValuePair(fieldType, values);
    }

    /**
     * Creates a multi-valued FieldValuePair holding zero values, representing an explicit empty
     * array ({@code "field": []}). It backs a zero-length, non-null LIST cell, which reads back as
     * {@code []} and so stays distinct from an absent field (a null cell).
     *
     * @param fieldType the mapped field type
     * @return an empty multi-valued pair
     */
    public static FieldValuePair emptyMultiValued(MappedFieldType fieldType) {
        return new FieldValuePair(fieldType, new ArrayList<>(0));
    }

    /**
     * Appends another value. Only valid on a multi-valued pair.
     *
     * @param nextValue the value to append
     */
    public void addValue(Object nextValue) {
        if (values == null) {
            throw new IllegalStateException("Cannot add a value to a single-valued FieldValuePair for [" + fieldType.name() + "]");
        }
        values.add(nextValue);
    }

    /**
     * Creates a pair whose values are positioned by the array element they came from.
     *
     * <p>Used for a leaf inside an array of objects, where the object is stored as
     * {@code LIST<STRUCT<..>>}. Position matters and gaps are meaningful: an element that omitted
     * this leaf leaves a null at its ordinal, which is what keeps
     * {@code [{"a":1},{"b":2}]} distinct from {@code [{"a":1,"b":2}]}.
     *
     * @param fieldType the mapped field type
     * @param value the value seen for element {@code elementOrdinal}
     * @param elementOrdinal zero-based index of the element that carried it
     * @return an element-indexed pair
     */
    public static FieldValuePair elementIndexed(MappedFieldType fieldType, Object value, int elementOrdinal) {
        FieldValuePair pair = new FieldValuePair(fieldType, new ArrayList<>(elementOrdinal + 1));
        pair.elementIndexed = true;
        pair.setElementValue(value, elementOrdinal);
        return pair;
    }

    /**
     * Records {@code value} as belonging to element {@code elementOrdinal}, padding any elements in
     * between with nulls so a leaf's position always matches its element.
     *
     * @param value the value
     * @param elementOrdinal zero-based index of the element that carried it
     */
    public void setElementValue(Object value, int elementOrdinal) {
        if (elementIndexed == false) {
            throw new IllegalStateException("Cannot set an element value on a non element-indexed pair for [" + fieldType.name() + "]");
        }
        while (values.size() <= elementOrdinal) {
            values.add(null);
        }
        values.set(elementOrdinal, value);
    }

    /** Returns whether this pair's values are positioned by array element. */
    public boolean isElementIndexed() {
        return elementIndexed;
    }

    /** Returns whether this pair accumulates multiple values into a list column. */
    public boolean isMultiValued() {
        return values != null;
    }

    /** Returns the number of values held: always 1 for a scalar pair. */
    public int valueCount() {
        return values == null ? 1 : values.size();
    }

    /**
     * Returns the field type.
     *
     * @return the mapped field type
     */
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    /**
     * Returns the value: the single parsed value, or the {@code List} of values for a
     * multi-valued pair.
     *
     * @return the parsed field value(s)
     */
    public Object getValue() {
        return values != null ? values : value;
    }
}
