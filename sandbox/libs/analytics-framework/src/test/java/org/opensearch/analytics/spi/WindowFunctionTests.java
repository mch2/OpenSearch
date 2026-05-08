/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.test.OpenSearchTestCase;

public class WindowFunctionTests extends OpenSearchTestCase {

    /** SUM is mapped to SqlKind.SUM so fromSqlKind can resolve a RexOver whose operator is SqlStdOperatorTable.SUM. */
    public void testSumMapsToSqlKindSum() {
        assertSame(WindowFunction.SUM, WindowFunction.fromSqlKind(SqlKind.SUM));
    }

    public void testRowNumberMapsToSqlKindRowNumber() {
        assertSame(WindowFunction.ROW_NUMBER, WindowFunction.fromSqlKind(SqlKind.ROW_NUMBER));
    }

    public void testFromSqlKindReturnsNullForUnmapped() {
        // CAST is a scalar, not a window function — must not resolve.
        assertNull(WindowFunction.fromSqlKind(SqlKind.CAST));
    }
}
