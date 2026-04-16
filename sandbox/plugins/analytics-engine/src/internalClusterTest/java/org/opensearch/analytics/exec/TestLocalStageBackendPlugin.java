/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.plugins.Plugin;

import java.util.Set;

/**
 * Test-only backend plugin that provides {@link TestSummingLocalStageContext}
 * for local stages. Declares {@link OperatorCapability#LOCAL_STAGE}
 * so the {@link org.opensearch.analytics.planner.CapabilityRegistry} selects it
 * for {@code LOCAL} stages.
 *
 * <p>Holds a {@code static volatile} reference to the last created context so
 * tests can inspect it after dispatch completes.
 */
public class TestLocalStageBackendPlugin extends Plugin implements AnalyticsSearchBackendPlugin {

    /** Last context instance created — tests grab this after dispatch. */
    public static volatile TestSummingLocalStageContext lastInstance;

    @Override
    public String name() {
        return "test-coord-reduce";
    }

    @Override
    public Set<OperatorCapability> supportedOperators() {
        return Set.of(OperatorCapability.LOCAL_STAGE);
    }

    @Override
    public LocalStageContext createLocalStage(LocalStageRequest req) {
        TestSummingLocalStageContext ctx = new TestSummingLocalStageContext(req);
        lastInstance = ctx;
        return ctx;
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        throw new UnsupportedOperationException("test-coord-reduce does not support shard-level execution");
    }
}
