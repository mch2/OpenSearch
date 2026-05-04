/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Objects;

/**
 * Abstract base class for Parquet field implementations that handle conversion
 * between OpenSearch field types and Apache Arrow vectors.
 */
public abstract class ParquetField {

    /** Creates a new ParquetField. */
    public ParquetField() {}

    /**
     * Writes the parsed field value into the appropriate vector in the managed VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    protected abstract void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue);

    /**
     * Creates and processes a field entry. Throws if vector not present in VSR.
     * @param fieldType the mapped field type
     * @param managedVSR the managed vector schema root
     * @param parseValue the parsed value to write
     */
    public final void createField(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        Objects.requireNonNull(fieldType, "MappedFieldType cannot be null");
        Objects.requireNonNull(managedVSR, "ManagedVSR cannot be null");
        if (managedVSR.getVector(fieldType.name()) != null) {
            addToGroup(fieldType, managedVSR, parseValue);
        } else {
            throw new IllegalArgumentException("Vector not found for field: " + fieldType.name());
        }
    }

    /** Returns the Arrow type for this field. */
    public abstract ArrowType getArrowType();

    /** Returns the Arrow field type with nullability metadata. */
    public abstract FieldType getFieldType();

    /**
     * Format-aware variant of {@link #getArrowType()}. The default implementation
     * delegates to {@link #getArrowType()} so existing ParquetField impls that don't
     * care about per-instance mapping metadata are unaffected.
     *
     * <p>Subclasses that need to branch on the {@link MappedFieldType} (e.g.
     * {@link org.opensearch.parquet.fields.core.data.date.DateParquetField}, which
     * picks Date32 / Time / Timestamp based on the format pattern) override this
     * method and use {@code null} to mean "no context; fall back to the broadest
     * Arrow type."
     *
     * @param mappedFieldType the field's mapping, or {@code null} if the caller has
     *                        no handle on it (e.g. legacy paths that predate format
     *                        awareness); overrides treat null as the default.
     */
    public ArrowType getArrowType(MappedFieldType mappedFieldType) {
        return getArrowType();
    }

    /**
     * Format-aware variant of {@link #getFieldType()}. See
     * {@link #getArrowType(MappedFieldType)} for semantics.
     */
    public FieldType getFieldType(MappedFieldType mappedFieldType) {
        return getFieldType();
    }
}
