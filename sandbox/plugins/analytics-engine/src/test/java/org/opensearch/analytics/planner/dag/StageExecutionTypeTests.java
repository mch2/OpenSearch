/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link StageExecutionType} enum values.
 *
 * Validates: Requirements 1.1
 */
public class StageExecutionTypeTests extends OpenSearchTestCase {

    /**
     * Verifies that the {@code LOCAL} value exists on the enum.
     */
    public void testLocalValueExists() {
        StageExecutionType local = StageExecutionType.LOCAL;
        assertNotNull("LOCAL enum value should exist", local);
        assertEquals("LOCAL", local.name());
    }

    /** Sanity: the enum has exactly two values: DATA_NODE and LOCAL. */
    public void testExactlyTwoValues() {
        assertNotNull(StageExecutionType.DATA_NODE);
        assertNotNull(StageExecutionType.LOCAL);
        assertEquals("StageExecutionType should have exactly 2 values", 2, StageExecutionType.values().length);
    }
}
