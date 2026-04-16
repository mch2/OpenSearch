/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for the default methods on {@link AnalyticsSearchBackendPlugin}.
 */
public class AnalyticsSearchBackendPluginTests extends OpenSearchTestCase {

    /**
     * Minimal stub that implements only the required abstract methods
     * from {@link SearchExecEngineProvider}, leaving all default methods
     * on {@link AnalyticsSearchBackendPlugin} untouched.
     */
    private static AnalyticsSearchBackendPlugin stubPlugin(String backendName) {
        return new AnalyticsSearchBackendPlugin() {
            @Override
            public String name() {
                return backendName;
            }

            @Override
            public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
                throw new UnsupportedOperationException("not implemented in stub");
            }
        };
    }

    private static LocalStageRequest dummyRequest() {
        return new LocalStageRequest(
            "q-1",
            0,
            new byte[] { 1 },
            mock(BufferAllocator.class),
            mock(ExchangeSink.class),
            Map.of()
        );
    }

    public void testCreateLocalStageDefaultThrows() {
        String backendName = "test-backend";
        AnalyticsSearchBackendPlugin plugin = stubPlugin(backendName);
        LocalStageRequest req = dummyRequest();

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> plugin.createLocalStage(req));
        assertEquals("Backend " + backendName + " does not support local stage execution", ex.getMessage());
    }

    public void testCreateLocalStageMessageIncludesBackendName() {
        String backendName = "my-custom-engine";
        AnalyticsSearchBackendPlugin plugin = stubPlugin(backendName);
        LocalStageRequest req = dummyRequest();

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> plugin.createLocalStage(req));
        assertTrue("Exception message should contain the backend name", ex.getMessage().contains(backendName));
    }
}
