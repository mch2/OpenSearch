/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

/**
 * Capability-declaration tests for {@link DataFusionAnalyticsBackendPlugin}.
 *
 * <p>The planner's {@code OpenSearchTableScanRule} walks every field of a scanned index
 * and drops any backend that doesn't declare a ScanCapability for that field's type.
 * Indices like {@code big5} include fields of type {@code match_only_text}; if the
 * datafusion backend (the only scan-capable backend today) does not list
 * {@link FieldType#MATCH_ONLY_TEXT} in its supported set, such indices cannot be scanned
 * and the planner throws "No backend can scan all requested fields on index [...]".
 *
 * <p>These tests pin the declared capability set so the regression stays caught.
 */
public class DataFusionAnalyticsBackendPluginTests extends OpenSearchTestCase {

    private BackendCapabilityProvider capabilityProvider() {
        DataFusionAnalyticsBackendPlugin backend = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        return backend.getCapabilityProvider();
    }

    /** {@code match_only_text} is a read-through alias for {@code text} in DF's parquet reader
     *  (see LuceneFieldFactoryRegistry.java — values are written as plain strings). The
     *  capability surface must mirror TEXT so big5-shape indices (which use
     *  {@code message: match_only_text} via dynamic templates) are scannable. */
    public void testScanCapabilitiesIncludeMatchOnlyText() {
        Set<FieldType> declared = new HashSet<>();
        for (ScanCapability cap : capabilityProvider().scanCapabilities()) {
            declared.addAll(cap.supportedFieldTypes());
        }
        assertTrue(
            "datafusion backend must declare MATCH_ONLY_TEXT in scanCapabilities so indices "
                + "with match_only_text fields (e.g. big5) can be scanned; declared = " + declared,
            declared.contains(FieldType.MATCH_ONLY_TEXT)
        );
        // TEXT must still be there — MATCH_ONLY_TEXT is additive.
        assertTrue("TEXT must also remain declared", declared.contains(FieldType.TEXT));
    }

    /** Filter capabilities should mirror scan capabilities on the text family — adding one
     *  field type to SUPPORTED_FIELD_TYPES adds it everywhere the set is iterated. */
    public void testFilterCapabilitiesIncludeMatchOnlyText() {
        boolean found = capabilityProvider().filterCapabilities().stream()
            .filter(cap -> cap instanceof FilterCapability.Standard)
            .map(cap -> ((FilterCapability.Standard) cap).fieldTypes())
            .anyMatch(types -> types.contains(FieldType.MATCH_ONLY_TEXT));
        assertTrue("datafusion backend must declare MATCH_ONLY_TEXT in filterCapabilities", found);
    }

    /** Aggregate capabilities should also cover MATCH_ONLY_TEXT (e.g. count/distinct_count). */
    public void testAggregateCapabilitiesIncludeMatchOnlyText() {
        boolean found = capabilityProvider().aggregateCapabilities().stream()
            .map(AggregateCapability::fieldTypes)
            .anyMatch(types -> types.contains(FieldType.MATCH_ONLY_TEXT));
        assertTrue("datafusion backend must declare MATCH_ONLY_TEXT in aggregateCapabilities", found);
    }

    /** Project/scalar capabilities should also cover MATCH_ONLY_TEXT. */
    public void testProjectCapabilitiesIncludeMatchOnlyText() {
        boolean found = capabilityProvider().projectCapabilities().stream()
            .filter(cap -> cap instanceof ProjectCapability.Scalar)
            .map(cap -> ((ProjectCapability.Scalar) cap).fieldTypes())
            .anyMatch(types -> types.contains(FieldType.MATCH_ONLY_TEXT));
        assertTrue("datafusion backend must declare MATCH_ONLY_TEXT in projectCapabilities", found);
    }

    /** Non-numeric singular / container types (binary, nested, object, flat_object,
     *  completion) appear in real-world mappings like the {@code datatypes_nonnumeric}
     *  fixture used by {@code CalciteDateTimeComparisonIT}. OpenSearchTableScanRule walks
     *  every mapped field on the source index — even when a query only touches a date
     *  column — and drops any backend that doesn't declare a ScanCapability for every type
     *  present. If datafusion (the only scan-capable backend) doesn't declare these, the
     *  whole scan fails with "No backend can scan all requested fields on index [...]".
     *
     *  <p>Declaring them is safe: the scan is just reading the column's doc-values blob
     *  through the parquet reader — DataFusion never interprets the contents beyond the
     *  query's projected expressions.
     *
     *  <p>Geo types (GEO_POINT / GEO_SHAPE / POINT / SHAPE) are intentionally excluded —
     *  geo is out of scope for analytics engine. */
    public void testScanCapabilitiesIncludeNonNumericContainerTypes() {
        Set<FieldType> declared = new HashSet<>();
        for (ScanCapability cap : capabilityProvider().scanCapabilities()) {
            declared.addAll(cap.supportedFieldTypes());
        }
        Set<FieldType> required = Set.of(
            FieldType.BINARY,
            FieldType.NESTED,
            FieldType.OBJECT,
            FieldType.FLAT_OBJECT,
            FieldType.COMPLETION
        );
        for (FieldType t : required) {
            assertTrue(
                "datafusion backend must declare " + t + " in scanCapabilities so indices "
                    + "with " + t.getMappingType() + " fields (e.g. datatypes_nonnumeric) can be scanned; declared = " + declared,
                declared.contains(t)
            );
        }
        // Geo types must remain NOT declared — out of scope.
        Set<FieldType> excluded = Set.of(
            FieldType.GEO_POINT,
            FieldType.GEO_SHAPE,
            FieldType.POINT,
            FieldType.SHAPE
        );
        for (FieldType t : excluded) {
            assertFalse(
                "datafusion backend must NOT declare " + t + " — geo is out of scope; declared = " + declared,
                declared.contains(t)
            );
        }
    }
}
