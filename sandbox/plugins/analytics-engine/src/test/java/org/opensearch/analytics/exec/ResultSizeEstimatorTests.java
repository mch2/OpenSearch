/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/** Tests for {@link ResultSizeEstimator}'s worst-case native result-size estimate. */
public class ResultSizeEstimatorTests extends OpenSearchTestCase {

    private static RelDataType rowType(SqlTypeName... types) {
        RelDataTypeFactory factory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataTypeFactory.Builder b = factory.builder();
        int i = 0;
        for (SqlTypeName t : types) {
            b.add("c" + i++, t);
        }
        return b.build();
    }

    /**
     * Per-column widths × row cap: fixed-width uses the Arrow buffer width + 1 validity byte;
     * variable-width uses the allowance + 4-byte offset + 1 validity byte.
     */
    public void testEstimateMixedWidths() {
        // BIGINT (8+1) + VARCHAR (allowance 256 + 4 + 1) = 9 + 261 = 270 per row.
        long perRow = (8 + 1) + (256 + 4 + 1);
        assertEquals(
            perRow * 100,
            ResultSizeEstimator.estimateWorstCaseResultBytes(rowType(SqlTypeName.BIGINT, SqlTypeName.VARCHAR), 100L, 256)
        );
    }

    /** Fixed-width widths: INT/FLOAT/DATE → 4+1, BIGINT/DOUBLE/TIMESTAMP → 8+1, BOOLEAN → 1+1. */
    public void testEstimateFixedWidths() {
        long perRow = (4 + 1) + (8 + 1) + (1 + 1);
        assertEquals(
            perRow * 10,
            ResultSizeEstimator.estimateWorstCaseResultBytes(
                rowType(SqlTypeName.INTEGER, SqlTypeName.DOUBLE, SqlTypeName.BOOLEAN),
                10L,
                256
            )
        );
    }

    /** rowCap is a long: a cap × per-row product beyond int range does not overflow. */
    public void testEstimateLongRowCapNoOverflow() {
        // single BIGINT column: 9 bytes/row; 1e9 rows → 9e9 bytes, which overflows int but not long.
        long est = ResultSizeEstimator.estimateWorstCaseResultBytes(rowType(SqlTypeName.BIGINT), 1_000_000_000L, 256);
        assertEquals(9_000_000_000L, est);
    }
}
