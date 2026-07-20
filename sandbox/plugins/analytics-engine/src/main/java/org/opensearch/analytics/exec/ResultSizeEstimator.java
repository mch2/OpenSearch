/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Estimates the worst-case native (pre-expansion) byte footprint of a query result, for the upfront
 * admission charge in {@code DefaultPlanExecutor}. Intentionally pessimistic — the ceiling that
 * {@code ResultHeapCharge.shrinkTo} later relaxes to the actual materialized size.
 */
final class ResultSizeEstimator {

    private ResultSizeEstimator() {}

    /**
     * {@code rowCap} rows of {@code rowType}. Per column: fixed-width types use their Arrow value-buffer
     * width (INT/FLOAT/DATE → 4B, BIGINT/DOUBLE/TIMESTAMP → 8B, BOOLEAN → 1B) plus 1 validity byte;
     * variable-width types (VARCHAR/VARBINARY/other) use {@code varWidthAllowanceBytes} plus 4 offset
     * bytes plus 1 validity byte. Returns {@code rowCap × Σ(per-column width)} as native bytes; the
     * caller applies the heap-expansion factor.
     */
    static long estimateWorstCaseResultBytes(RelDataType rowType, long rowCap, long varWidthAllowanceBytes) {
        long perRow = 0;
        for (RelDataTypeField field : rowType.getFieldList()) {
            perRow += perColumnWidthBytes(field.getType().getSqlTypeName(), varWidthAllowanceBytes);
        }
        return rowCap * perRow;
    }

    private static long perColumnWidthBytes(SqlTypeName type, long varWidthAllowanceBytes) {
        final long validity = 1L;
        switch (type) {
            case BOOLEAN:
            case TINYINT:
                return 1L + validity;
            case SMALLINT:
                return 2L + validity;
            case INTEGER:
            case FLOAT:
            case REAL:
            case DATE:
            case TIME:
                return 4L + validity;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 8L + validity;
            default:
                // variable-width (VARCHAR, VARBINARY, CHAR, and anything else): allowance + offset + validity
                return varWidthAllowanceBytes + 4L + validity;
        }
    }
}
