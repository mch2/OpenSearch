/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Legacy transport action for shard-level fragment execution.
 * Superseded by the streaming handler registered in
 * {@code AnalyticsSearchTransportService} which uses
 * {@code FragmentExecutionAction.NAME}.
 */
public class TransportAnalyticsShardAction extends HandledTransportAction<FragmentExecutionRequest, FragmentExecutionResponse> {

    public static final String ACTION_NAME = "indices:data/read/analytics/shard";

    private final IndicesService indicesService;
    private final AnalyticsSearchService searchService;

    @Inject
    public TransportAnalyticsShardAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        AnalyticsSearchService searchService
    ) {
        super(ACTION_NAME, transportService, actionFilters, FragmentExecutionRequest::new);
        this.indicesService = indicesService;
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, FragmentExecutionRequest request, ActionListener<FragmentExecutionResponse> listener) {
        // Legacy action — fragment execution now uses the streaming path registered in
        // AnalyticsSearchTransportService (FragmentExecutionAction.NAME).
        listener.onFailure(
            new UnsupportedOperationException(
                "AnalyticsShardAction is superseded by FragmentExecutionAction with streaming transport"
            )
        );
    }
}
