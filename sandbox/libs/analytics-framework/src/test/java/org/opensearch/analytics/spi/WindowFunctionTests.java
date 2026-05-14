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

/**
 * Asserts each {@link WindowFunction} entry maps to its Calcite {@link SqlKind} and
 * {@link WindowFunction#fromSqlKind} round-trips that mapping.
 */
public class WindowFunctionTests extends OpenSearchTestCase {

    public void testAggregateOverKinds() {
        assertEquals(WindowFunction.SUM, WindowFunction.fromSqlKind(SqlKind.SUM));
        assertEquals(WindowFunction.COUNT, WindowFunction.fromSqlKind(SqlKind.COUNT));
        assertEquals(WindowFunction.AVG, WindowFunction.fromSqlKind(SqlKind.AVG));
        assertEquals(WindowFunction.MIN, WindowFunction.fromSqlKind(SqlKind.MIN));
        assertEquals(WindowFunction.MAX, WindowFunction.fromSqlKind(SqlKind.MAX));
    }

    public void testUnsupportedKindReturnsNull() {
        // Ranking functions are not on this route yet — streamstats (which lowers to them)
        // isn't wired through analytics-engine. Verifying the enum doesn't claim them.
        assertNull(WindowFunction.fromSqlKind(SqlKind.ROW_NUMBER));
        assertNull(WindowFunction.fromSqlKind(SqlKind.RANK));
        assertNull(WindowFunction.fromSqlKind(SqlKind.DENSE_RANK));
        assertNull(WindowFunction.fromSqlKind(SqlKind.LAG));
        assertNull(WindowFunction.fromSqlKind(SqlKind.LEAD));
        assertNull(WindowFunction.fromSqlKind(SqlKind.NTILE));
    }
}
