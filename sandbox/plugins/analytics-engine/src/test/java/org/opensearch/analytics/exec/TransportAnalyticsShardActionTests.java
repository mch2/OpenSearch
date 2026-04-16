/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TransportAnalyticsShardAction} and {@link AnalyticsShardAction}.
 */
public class TransportAnalyticsShardActionTests extends OpenSearchTestCase {

    public void testActionName() {
        assertEquals("indices:data/read/analytics/shard", AnalyticsShardAction.NAME);
    }

    @SuppressWarnings("unchecked")
    public void testDoExecuteDelegatesToSearchService() {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);

        ShardId shardId = new ShardId(new Index("test_index", "_na_"), 0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "hello", 42L });
        FragmentExecutionResponse expectedResponse = new FragmentExecutionResponse(List.of("col1", "col2"), rows);
        when(searchService.executeFragment(any(), any(), any())).thenReturn(expectedResponse);

        TransportAnalyticsShardAction action = createAction(indicesService, searchService);

        FragmentExecutionRequest request = new FragmentExecutionRequest(
            "query-1",
            0,
            shardId,
            List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null))
        );

        ActionListener<FragmentExecutionResponse> listener = mock(ActionListener.class);
        action.doExecute(mock(Task.class), request, listener);

        verify(listener).onResponse(expectedResponse);
    }

    @SuppressWarnings("unchecked")
    public void testDoExecuteCallsOnFailureWhenShardNotFound() {
        IndicesService indicesService = mock(IndicesService.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);

        when(indicesService.indexServiceSafe(any())).thenThrow(new IndexNotFoundException("missing_index"));

        TransportAnalyticsShardAction action = createAction(indicesService, searchService);

        ShardId shardId = new ShardId(new Index("missing_index", "_na_"), 0);
        FragmentExecutionRequest request = new FragmentExecutionRequest(
            "query-2",
            0,
            shardId,
            List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null))
        );

        ActionListener<FragmentExecutionResponse> listener = mock(ActionListener.class);
        action.doExecute(mock(Task.class), request, listener);

        verify(listener).onFailure(any(IndexNotFoundException.class));
    }

    @SuppressWarnings("unchecked")
    public void testDoExecuteCallsOnFailureWhenExecutionFails() {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        AnalyticsSearchService searchService = mock(AnalyticsSearchService.class);

        ShardId shardId = new ShardId(new Index("test_index", "_na_"), 0);
        when(indicesService.indexServiceSafe(any())).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);
        when(searchService.executeFragment(any(), any(), any())).thenThrow(new RuntimeException("execution failed"));

        TransportAnalyticsShardAction action = createAction(indicesService, searchService);

        FragmentExecutionRequest request = new FragmentExecutionRequest(
            "query-3",
            0,
            shardId,
            List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null))
        );

        ActionListener<FragmentExecutionResponse> listener = mock(ActionListener.class);
        action.doExecute(mock(Task.class), request, listener);

        verify(listener).onFailure(any(RuntimeException.class));
    }

    private TransportAnalyticsShardAction createAction(IndicesService indicesService, AnalyticsSearchService searchService) {
        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        return new TransportAnalyticsShardAction(transportService, actionFilters, indicesService, searchService);
    }
}
