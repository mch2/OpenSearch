/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Verifies the {@link StageExecution} interface contract:
 * <ul>
 *   <li>{@link FanOutStageExecution} implements {@link StageExecution}</li>
 *   <li>{@link StageExecution.State#CANCELLED} exists in the enum</li>
 * </ul>
 *
 * Validates: Requirements 4.1, 4.2
 */
public class StageExecutionInterfaceTests extends OpenSearchTestCase {

    /**
     * FanOutStageExecution must implement the StageExecution interface.
     */
    public void testFanOutImplementsInterface() {
        assertTrue("FanOutStageExecution must implement StageExecution", StageExecution.class.isAssignableFrom(FanOutStageExecution.class));
    }

    /**
     * The State enum on the StageExecution interface must contain CANCELLED.
     */
    public void testCancelledStateExists() {
        StageExecution.State cancelled = StageExecution.State.valueOf("CANCELLED");
        assertNotNull("CANCELLED state must exist", cancelled);
        assertEquals(StageExecution.State.CANCELLED, cancelled);
    }

    /**
     * The State enum must contain all expected values including the new CANCELLED value.
     */
    public void testAllStatesPresent() {
        StageExecution.State[] states = StageExecution.State.values();
        assertEquals("State enum must have 6 values", 6, states.length);
        assertNotNull(StageExecution.State.CREATED);
        assertNotNull(StageExecution.State.RUNNING);
        assertNotNull(StageExecution.State.TERMINATED);
        assertNotNull(StageExecution.State.SUCCEEDED);
        assertNotNull(StageExecution.State.FAILED);
        assertNotNull(StageExecution.State.CANCELLED);
    }

    /**
     * The StageExecution interface must declare the expected methods.
     */
    public void testInterfaceMethodsExist() throws NoSuchMethodException {
        assertNotNull(StageExecution.class.getMethod("getStageId"));
        assertNotNull(StageExecution.class.getMethod("getState"));
        assertNotNull(StageExecution.class.getMethod("getMetrics"));
        assertNotNull(StageExecution.class.getMethod("cancel", String.class));
    }
}
