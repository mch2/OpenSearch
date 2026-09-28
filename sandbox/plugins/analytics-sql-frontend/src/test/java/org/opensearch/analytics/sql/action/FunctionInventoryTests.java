/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql.action;

import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsOperatorTables;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Drift guard (R4, R5): every entry in the {@link ScalarFunction} / {@link AggregateFunction} /
 * {@link WindowFunction} SPI enums must be EITHER validator-resolvable (through AE's published
 * operator table or the chained standard table) OR recorded in
 * {@link AnalyticsOperatorTables#KNOWN_UNSUPPORTED_V1} — never both, never neither. An entry that
 * violates this partition fails the test, forcing a conscious wire-or-list decision on every future
 * AE function and keeping {@code AE-PACKAGING.md} honest against the same source.
 *
 * <p>Pure enum/predicate lookup — no schema, no planner, no cluster, no Arrow.
 */
public class FunctionInventoryTests extends OpenSearchTestCase {

    public void testEveryEnumEntryIsResolvableXorListedUnsupported() {
        List<String> offenders = new ArrayList<>();

        for (ScalarFunction fn : ScalarFunction.values()) {
            check("scalar:" + fn.name(), AnalyticsOperatorTables.isResolvable(fn), offenders);
        }
        for (AggregateFunction fn : AggregateFunction.values()) {
            check("aggregate:" + fn.name(), AnalyticsOperatorTables.isResolvable(fn), offenders);
        }
        for (WindowFunction fn : WindowFunction.values()) {
            check("window:" + fn.name(), AnalyticsOperatorTables.isResolvable(fn), offenders);
        }

        assertTrue(
            "Every enum entry must be exactly one of: validator-resolvable, or listed in "
                + "KNOWN_UNSUPPORTED_V1. Offenders: "
                + offenders,
            offenders.isEmpty()
        );
    }

    /** A concrete aggregate (COUNT) resolves; a state-expanding one (ARG_MIN) is listed unsupported. */
    public void testKnownEndpoints() {
        assertTrue(AnalyticsOperatorTables.isResolvable(AggregateFunction.COUNT));
        assertFalse(AnalyticsOperatorTables.isResolvable(AggregateFunction.ARG_MIN));
        assertTrue(AnalyticsOperatorTables.KNOWN_UNSUPPORTED_V1.contains("aggregate:ARG_MIN"));
    }

    private static void check(String key, boolean resolvable, List<String> offenders) {
        boolean listed = AnalyticsOperatorTables.KNOWN_UNSUPPORTED_V1.contains(key);
        if (resolvable == listed) {
            offenders.add(key + " (resolvable=" + resolvable + ", listedUnsupported=" + listed + ")");
        }
    }
}
