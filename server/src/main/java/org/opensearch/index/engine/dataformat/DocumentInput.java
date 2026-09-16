/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Represents a document input for adding fields and metadata to a writer.
 *
 * @param <T> the type of the final input representation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    /** Standard field name for the row ID used to correlate documents across data formats. */
    String ROW_ID_FIELD = "__row_id__";

    /**
     * Gets the final input representation.
     *
     * @return the final input of type T
     */
    T getFinalInput();

    /**
     * Adds a field to the document.
     *
     * @param fieldType the mapped field type
     * @param value the field value
     */
    void addField(MappedFieldType fieldType, Object value);

    /**
     * Adds a field that belongs to one element of an array of objects.
     *
     * <p>Row-oriented formats do not care which element a value came from — Lucene flattens the
     * array either way — but a columnar format that stores the array as {@code LIST<STRUCT<..>>}
     * does: two sibling leaves are the same element only if they share an ordinal, and an element
     * that omitted a leaf has to become a null inside that element rather than a shortened list.
     * Without the ordinal, {@code [{"name":"a"},{"name":"b","time":2}]} and
     * {@code [{"name":"a","time":2},{"name":"b"}]} are indistinguishable.
     *
     * <p>Defaults to dropping the ordinal, so a format that flattens is unaffected.
     *
     * @param fieldType the mapped field type
     * @param value the field value
     * @param elementOrdinal zero-based index of the enclosing array element
     */
    default void addField(MappedFieldType fieldType, Object value, int elementOrdinal) {
        addField(fieldType, value);
    }

    /**
     * Records how many elements an object arrived with as an array.
     *
     * <p>The count cannot be inferred from the values written, because an element carries no value of
     * its own: {@code [{"name":"a"},{}]} writes one {@code name} and would look like a one-element
     * array, and {@code []} and {@code [{},{}]} would both look like no array at all. Only the parser
     * knows, so it says so — a columnar format storing the object as {@code LIST<STRUCT<..>>} then
     * writes a run of exactly that length, and formats that flatten arrays ignore it.
     *
     * @param objectPath dotted path of the object
     * @param elementCount number of elements the document supplied, possibly zero
     */
    default void addObjectArray(String objectPath, int elementCount) {}

    /**
     * Adds a row ID field to the document.
     *
     * @param rowIdFieldName the name of the row ID field
     * @param rowId the row ID value
     */
    void setRowId(String rowIdFieldName, long rowId);

    /**
     * Given a field name, returns the number of values associated with that field in the document.
     * @param fieldName name of the field to lookup
     * @return count of field values
     */
    long getFieldCount(String fieldName);
}
