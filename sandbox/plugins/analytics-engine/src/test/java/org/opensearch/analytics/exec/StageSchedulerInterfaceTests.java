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
 * Validates that {@link StageScheduler} is a functional interface with
 * exactly one abstract method ({@code schedule}).
 */
public class StageSchedulerInterfaceTests extends OpenSearchTestCase {

    /**
     * Assigns a no-op lambda to {@link StageScheduler}, proving the interface
     * has exactly one abstract method and the lambda can be called with all
     * seven parameters returning void.
     */
    public void testFunctionalShape() {
        StageScheduler scheduler = (stage, outputSink, client, childDispatcher, config, state, listener) -> {
            // no-op — just proves the lambda compiles and is callable
        };

        // Call the lambda with nulls to verify it executes without error
        scheduler.schedule(null, null, null, null, null, null, null);
    }
}
