/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumMap;
import java.util.Map;

/**
 * Unit tests for {@link ScalarFunction}.
 */
public class ScalarFunctionTests extends OpenSearchTestCase {

    /** Non-OTHER_FUNCTION SqlKinds must be unique: fromSqlKind picks the first match and would shadow later entries. */
    public void testNoDuplicateSqlKindBindings() {
        Map<SqlKind, ScalarFunction> claimedBy = new EnumMap<>(SqlKind.class);
        for (ScalarFunction func : ScalarFunction.values()) {
            SqlKind kind = func.getSqlKind();
            if (kind == SqlKind.OTHER_FUNCTION) {
                continue;
            }
            ScalarFunction existing = claimedBy.put(kind, func);
            if (existing != null) {
                fail("SqlKind." + kind + " claimed by both " + existing + " and " + func);
            }
        }
    }

    public void testSargPredicateIsBoundToSqlKindSearch() {
        assertSame(ScalarFunction.SARG_PREDICATE, ScalarFunction.fromSqlKind(SqlKind.SEARCH));
    }

    /**
     * Callers (e.g. {@code OpenSearchProjectRule}) guard on a null return from
     * {@link ScalarFunction#fromSqlFunction(SqlFunction)}. The contract (per the
     * javadoc) must return null — not throw — when the function name does not
     * match any enum constant. Without this contract, any unknown scalar
     * function (e.g. {@code YEAR} before its adapter lands) short-circuits the
     * Hep planner rule with an IllegalArgumentException.
     */
    public void testFromSqlFunctionReturnsNullForUnknownName() {
        // MONTH is a valid Calcite operator the enum does not model (PR10 added
        // YEAR/CONVERT_TZ/UNIX_TIMESTAMP; MONTH is representative of any
        // date-part function routed through the name-based path that the enum
        // does not model yet).
        SqlFunction month = (SqlFunction) SqlStdOperatorTable.MONTH;
        ScalarFunction resolved = ScalarFunction.fromSqlFunction(month);
        assertNull("fromSqlFunction must return null for unknown names", resolved);
    }

    public void testFromSqlFunctionResolvesKnownName() {
        // UPPER is a well-known scalar function — valueOf("UPPER") succeeds.
        SqlFunction upper = (SqlFunction) SqlStdOperatorTable.UPPER;
        ScalarFunction resolved = ScalarFunction.fromSqlFunction(upper);
        assertNotNull("fromSqlFunction must resolve known names", resolved);
        assertSame(ScalarFunction.UPPER, resolved);
    }
}
