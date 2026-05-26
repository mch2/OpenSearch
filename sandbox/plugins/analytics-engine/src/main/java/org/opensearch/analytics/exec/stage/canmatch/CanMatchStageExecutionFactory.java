/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.canmatch;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.List;

/**
 * Builds a {@link CanMatchStage} from the Stage object built by {@code DAGBuilder}.
 *
 * <p>{@code sink} is ignored — can-match doesn't produce data, only metadata published via
 * {@link CanMatchStage#publishedMetadata()}.
 *
 * @opensearch.internal
 */
public final class CanMatchStageExecutionFactory implements StageExecutionFactory {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService dispatcher;

    public CanMatchStageExecutionFactory(ClusterService clusterService, AnalyticsSearchTransportService dispatcher) {
        this.clusterService = clusterService;
        this.dispatcher = dispatcher;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        List<CanMatchFilter> filters = stage.getCanMatchFilters();
        String backendId = resolveBackendId(stage);
        return new CanMatchStage(stage, config, clusterService, dispatcher, filters, backendId);
    }

    /** Pull backendId from the parent's plan alternatives (first one wins; canmatch is backend-routed). */
    private static String resolveBackendId(Stage stage) {
        List<StagePlan> plans = stage.getPlanAlternatives();
        return (plans == null || plans.isEmpty()) ? null : plans.get(0).backendId();
    }
}
