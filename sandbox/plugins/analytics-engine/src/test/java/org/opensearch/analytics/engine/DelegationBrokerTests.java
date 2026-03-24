/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.opensearch.analytics.delegation.DelegationBroker;
import org.opensearch.analytics.delegation.DelegationType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link DelegationBroker} and {@link DelegationType}.
 */
public class DelegationBrokerTests extends OpenSearchTestCase {

    public void testDelegationTypeHasFilterAndScan() {
        assertNotNull(DelegationType.FILTER);
        assertNotNull(DelegationType.SCAN);
        assertEquals(2, DelegationType.values().length);
    }
}
