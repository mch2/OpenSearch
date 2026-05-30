/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FieldType} type-name mapping.
 */
public class FieldTypeTests extends OpenSearchTestCase {

    /**
     * A {@code percentile(x, 50)} call lowers to a Calcite aggregate whose flag/spec
     * argument is a {@link SqlTypeName#SYMBOL} enum literal, projected as its own column.
     * That column must resolve to a concrete {@link FieldType} (it is a string-like enum
     * constant) so {@code FieldStorageInfo.resolve} doesn't throw "Unrecognized field type
     * [SYMBOL]". Maps to KEYWORD.
     */
    public void testSymbolMapsToKeyword() {
        assertEquals(FieldType.KEYWORD, FieldType.fromSqlTypeName(SqlTypeName.SYMBOL));
    }

    public void testNullSqlTypeNameReturnsNull() {
        assertNull(FieldType.fromSqlTypeName(null));
    }
}
