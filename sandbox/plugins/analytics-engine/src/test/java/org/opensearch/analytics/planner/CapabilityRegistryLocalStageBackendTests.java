/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CapabilityRegistry#selectLocalStageBackend}.
 *
 * Validates: Requirements 7.1
 */
public class CapabilityRegistryLocalStageBackendTests extends OpenSearchTestCase {

    /**
     * selectLocalStageBackend returns the first backend from plan alternatives
     * that has LOCAL_STAGE capability.
     */
    public void testReturnsBackendWithLocalStageCapability() {
        AnalyticsSearchBackendPlugin capable = mockBackend("datafusion", Set.of(OperatorCapability.LOCAL_STAGE));
        AnalyticsSearchBackendPlugin incapable = mockBackend("lucene", Set.of(OperatorCapability.SCAN));

        CapabilityRegistry registry = new CapabilityRegistry(List.of(capable, incapable), idx -> null, Map.of());

        String result = registry.selectLocalStageBackend(List.of("datafusion", "lucene"));
        assertEquals("datafusion", result);
    }

    /**
     * selectLocalStageBackend returns null when no backend in the alternatives
     * has the required capability.
     */
    public void testReturnsNullWhenNoEligibleBackend() {
        AnalyticsSearchBackendPlugin incapable = mockBackend("lucene", Set.of(OperatorCapability.SCAN));

        CapabilityRegistry registry = new CapabilityRegistry(List.of(incapable), idx -> null, Map.of());

        String result = registry.selectLocalStageBackend(List.of("lucene"));
        assertNull(result);
    }

    /**
     * selectLocalStageBackend picks the first matching backend in plan-alternative
     * order, not in registration order.
     */
    public void testRespectsAlternativeOrdering() {
        AnalyticsSearchBackendPlugin backendA = mockBackend("backend-a", Set.of(OperatorCapability.LOCAL_STAGE));
        AnalyticsSearchBackendPlugin backendB = mockBackend("backend-b", Set.of(OperatorCapability.LOCAL_STAGE));

        CapabilityRegistry registry = new CapabilityRegistry(List.of(backendA, backendB), idx -> null, Map.of());

        // backend-b listed first in alternatives -> should be selected
        String result = registry.selectLocalStageBackend(List.of("backend-b", "backend-a"));
        assertEquals("backend-b", result);
    }

    /**
     * localStageBackends returns the list of backends with LOCAL_STAGE capability.
     */
    public void testLocalStageBackendsReturnsEligible() {
        AnalyticsSearchBackendPlugin capable = mockBackend("datafusion", Set.of(OperatorCapability.LOCAL_STAGE));

        CapabilityRegistry registry = new CapabilityRegistry(List.of(capable), idx -> null, Map.of());

        List<String> alternatives = List.of("datafusion");
        assertNotNull(registry.selectLocalStageBackend(alternatives));
    }

    private AnalyticsSearchBackendPlugin mockBackend(String name, Set<OperatorCapability> operators) {
        AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
        when(backend.name()).thenReturn(name);
        when(backend.supportedOperators()).thenReturn(operators);
        return backend;
    }
}
