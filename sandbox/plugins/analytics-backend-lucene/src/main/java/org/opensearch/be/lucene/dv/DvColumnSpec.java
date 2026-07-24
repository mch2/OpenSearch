/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.spi.DocValuesLeafUnsupportedException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * One projected column of a doc-values leaf: the Arrow field (from the coordinator-advertised
 * projected schema — authoritative for the output type) plus the mapping-derived decode kind.
 *
 * <p>The v1 type table (spec J3): long/integer/short/byte → NUMERIC raw long; double/float →
 * NUMERIC sortable-bits; date → NUMERIC epoch-millis; keyword → SORTED ordinals; boolean →
 * NUMERIC 0/1. Everything else fails fragment-open with a typed
 * {@link DocValuesLeafUnsupportedException} — never a silent wrong answer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record DvColumnSpec(Field arrowField, DecodeKind kind) {

    /** How the column's doc values decode into its Arrow vector. */
    public enum DecodeKind {
        /** Raw long from NUMERIC/SORTED_NUMERIC (long/integer/short/byte, date millis, boolean 0/1). */
        NUMERIC_LONG,
        /** {@code NumericUtils.sortableLongToDouble} decode (double mapping). */
        NUMERIC_SORTABLE_DOUBLE,
        /** {@code NumericUtils.sortableIntToFloat} decode (float mapping). */
        NUMERIC_SORTABLE_FLOAT,
        /** SORTED/SORTED_SET ordinal → term bytes (keyword). */
        KEYWORD_ORD
    }

    private static final Set<String> RAW_LONG_TYPES = Set.of("long", "integer", "short", "byte", "date", "boolean");

    /**
     * Derives the column specs for {@code projectedSchema} against the index mapping, enforcing the
     * v1 scope table. Order follows the projected schema (positional binding downstream).
     *
     * @throws DocValuesLeafUnsupportedException on any v1 exclusion
     */
    public static List<DvColumnSpec> derive(org.apache.arrow.vector.types.pojo.Schema projectedSchema, MapperService mapperService) {
        List<DvColumnSpec> specs = new ArrayList<>(projectedSchema.getFields().size());
        for (Field field : projectedSchema.getFields()) {
            specs.add(deriveColumn(field, mapperService));
        }
        return specs;
    }

    private static DvColumnSpec deriveColumn(Field field, MapperService mapperService) {
        String name = field.getName();
        MappedFieldType fieldType = mapperService.fieldType(name);
        if (fieldType == null) {
            throw new DocValuesLeafUnsupportedException(
                DocValuesLeafUnsupportedException.Reason.UNKNOWN_FIELD,
                "projected column [" + name + "] not found in index mapping"
            );
        }
        if (fieldType.hasDocValues() == false) {
            throw new DocValuesLeafUnsupportedException(
                DocValuesLeafUnsupportedException.Reason.NO_DOC_VALUES,
                "field [" + name + "] of type [" + fieldType.typeName() + "] has no doc values"
            );
        }
        String typeName = fieldType.typeName();
        if (RAW_LONG_TYPES.contains(typeName)) {
            return new DvColumnSpec(field, DecodeKind.NUMERIC_LONG);
        }
        return switch (typeName) {
            case "double" -> new DvColumnSpec(field, DecodeKind.NUMERIC_SORTABLE_DOUBLE);
            case "float" -> new DvColumnSpec(field, DecodeKind.NUMERIC_SORTABLE_FLOAT);
            case "keyword" -> new DvColumnSpec(field, DecodeKind.KEYWORD_ORD);
            default -> throw new DocValuesLeafUnsupportedException(
                DocValuesLeafUnsupportedException.Reason.UNSUPPORTED_FIELD_TYPE,
                "field [" + name + "] of type [" + typeName + "] is outside the doc-values leaf v1 scope"
            );
        };
    }
}
