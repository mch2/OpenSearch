/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream.collector;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Adaptor class to map an OpenSearch field to an Arrow field.
 */
@ExperimentalApi
public class ArrowFieldAdaptor {
    public String getFieldName() {
        return fieldName;
    }

    public ArrowType getArrowType() {
        return arrowType;
    }

    String fieldName;
    ArrowType arrowType;
    String fieldType;

    public ArrowFieldAdaptor(String fieldName, ArrowType arrowType, String fieldType) {
        this.fieldName = fieldName;
        this.arrowType = arrowType;
        this.fieldType = fieldType;
    }

    /**
     * Gets the appropriate DocValues for this field based on its OpenSearch field type.
     *
     * @param leafReader The LeafReader to get DocValues from
     * @return The appropriate DocValues object for this field
     * @throws IOException If there's an error reading from the index
     * @throws IllegalArgumentException If the field type is not supported
     */
    public DocValuesType getDocValues(LeafReader leafReader) throws IOException {
        switch (fieldType.toLowerCase()) {
            case "long":
            case "integer":
            case "short":
            case "byte":
            case "double":
            case "float":
            case "date":
                return new NumericDocValuesType(leafReader.getSortedNumericDocValues(fieldName));

            case "keyword":
            case "ip":
            case "boolean":
                return new SortedDocValuesType(leafReader.getSortedSetDocValues(fieldName));

            default:
                throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }
    }

    // TODO bowen can use lucene DocValuesType?
    /**
     * Interface for DocValues types supported by Arrow.
     */
    @ExperimentalApi
    public interface DocValuesType {}

    /**
     * Class for NumericDocValues types supported by Arrow.
     */
    public static class NumericDocValuesType implements DocValuesType {
        private final SortedNumericDocValues numericDocValues;

        public NumericDocValuesType(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        public SortedNumericDocValues getNumericDocValues() {
            return numericDocValues;
        }
    }

    /**
     * Class for SortedDocValues types supported by Arrow.
     */
    public static class SortedDocValuesType implements DocValuesType {
        private final SortedSetDocValues sortedDocValues;

        public SortedDocValuesType(SortedSetDocValues sortedDocValues) {
            this.sortedDocValues = sortedDocValues;
        }

        public SortedSetDocValues getSortedDocValues() {
            return sortedDocValues;
        }
    }

    private static final Map<String, ArrowType> typeMap;

    static {
        typeMap = new HashMap<>();

        // Numeric types
        typeMap.put("long", new ArrowType.Int(64, true));
        typeMap.put("integer", new ArrowType.Int(32, true));
        typeMap.put("short", new ArrowType.Int(16, true));
        typeMap.put("byte", new ArrowType.Int(8, true));
        typeMap.put("double", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        typeMap.put("float", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));

        // Date type
        typeMap.put("date", new ArrowType.Date(DateUnit.MILLISECOND));

        // Keyword type (assuming it's represented as a string in Arrow)
        typeMap.put("keyword", new ArrowType.Utf8());

        // Boolean type
        typeMap.put("boolean", new ArrowType.Bool());

        // IP type (assuming it's represented as a fixed size binary in Arrow)
        typeMap.put("ip", new ArrowType.FixedSizeBinary(16)); // IPv6 address length
    }

    /**
     * Gets the appropriate ArrowType for a given OpenSearch field type.
     *
     * @param openSearchType The OpenSearch field type
     * @return The corresponding ArrowType
     * @throws IllegalArgumentException If the field type is not supported
     */
    public static ArrowType getArrowType(String openSearchType) {
        ArrowType arrowType = typeMap.get(openSearchType.toLowerCase());
        if (arrowType == null) {
            throw new IllegalArgumentException("Unsupported OpenSearch type: " + openSearchType);
        }
        return arrowType;
    }
}
