/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.TransportAnalyticsShardAction;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for cancellation plumbing in {@link TransportAnalyticsShardAction}.
 */
public class TransportAnalyticsShardActionCancellationTests extends OpenSearchTestCase {

    private TransportAnalyticsShardAction createAction(IndicesService indicesService, AnalyticsSearchService searchService) {
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        return new TransportAnalyticsShardAction(transportService, actionFilters, indicesService, searchService);
    }

    private FragmentExecutionRequest createRequest(ShardId shardId) {
        return new FragmentExecutionRequest("query-1", 0, shardId, List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null)));
    }

    @SuppressWarnings("unchecked")
    public void testDoExecutePassesShardTaskToSearchService() {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);

        ShardId shardId = new ShardId(new Index("test_index", "_na_"), 0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "val" });
        ScanResponse expectedResponse = new ScanResponse(List.of("col1"), rows);
        when(searchService.executeFragment(any(), any(), any())).thenReturn(expectedResponse);

        TransportAnalyticsShardAction action = createAction(indicesService, searchService);

        AnalyticsShardTask shardTask = new AnalyticsShardTask(
            1L,
            "transport",
            "indices:data/read/analytics/shard",
            "test",
            new org.opensearch.core.tasks.TaskId("node1", 0),
            Map.of()
        );

        FragmentExecutionRequest request = createRequest(shardId);
        ActionListener<ScanResponse> listener = mock(ActionListener.class);

        action.doExecute(shardTask, request, listener);

        verify(searchService).executeFragment(eq(request), same(indexShard), same(shardTask));
        verify(listener).onResponse(expectedResponse);
    }

    @SuppressWarnings("unchecked")
    public void testDoExecuteForwardsTaskCancelledExceptionToListener() {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);

        ShardId shardId = new ShardId(new Index("test_index", "_na_"), 0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);

        TaskCancelledException cancelledException = new TaskCancelledException("task cancelled: by user");
        when(searchService.executeFragment(any(), any(), any())).thenThrow(cancelledException);

        TransportAnalyticsShardAction action = createAction(indicesService, searchService);

        AnalyticsShardTask shardTask = new AnalyticsShardTask(
            1L,
            "transport",
            "indices:data/read/analytics/shard",
            "test",
            new org.opensearch.core.tasks.TaskId("node1", 0),
            Map.of()
        );

        FragmentExecutionRequest request = createRequest(shardId);
        ActionListener<ScanResponse> listener = mock(ActionListener.class);

        action.doExecute(shardTask, request, listener);

        verify(listener).onFailure(same(cancelledException));
    }
}
