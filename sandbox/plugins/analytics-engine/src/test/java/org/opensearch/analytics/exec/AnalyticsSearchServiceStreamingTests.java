/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportChannel;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AnalyticsSearchService#executeFragmentStreaming}.
 * Validates: Requirements 1.5
 */
public class AnalyticsSearchServiceStreamingTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "test-backend";
    private static final ShardId SHARD_ID = new ShardId(new Index("test_index", "_na_"), 0);

    @SuppressWarnings("unchecked")
    private IndexShard mockShard(SearchExecEngine<ExecutionContext, EngineResultStream> engine) throws Exception {
        IndexReaderProvider.Reader reader = mock(IndexReaderProvider.Reader.class);
        GatedCloseable<IndexReaderProvider.Reader> gatedReader = new GatedCloseable<>(reader, () -> {});

        DataFormatAwareEngine compositeEngine = mock(DataFormatAwareEngine.class);
        when(compositeEngine.acquireReader()).thenReturn(gatedReader);

        IndexShard shard = mock(IndexShard.class);
        when(shard.getCompositeEngine()).thenReturn(compositeEngine);
        when(shard.shardId()).thenReturn(SHARD_ID);

        return shard;
    }

    private FragmentExecutionRequest createRequest() {
        return new FragmentExecutionRequest(
            "query-1",
            0,
            SHARD_ID,
            List.of(new FragmentExecutionRequest.PlanAlternative(BACKEND_ID, new byte[] { 1, 2, 3 }))
        );
    }

    @SuppressWarnings("unchecked")
    private AnalyticsSearchBackendPlugin mockBackend(EngineResultStream stream) throws Exception {
        SearchExecEngine<ExecutionContext, EngineResultStream> engine = mock(SearchExecEngine.class);
        when(engine.execute(any(ExecutionContext.class))).thenReturn(stream);

        AnalyticsSearchBackendPlugin backend = mock(AnalyticsSearchBackendPlugin.class);
        when(backend.createSearchExecEngine(any(ExecutionContext.class))).thenReturn(engine);
        when(backend.name()).thenReturn(BACKEND_ID);

        return backend;
    }

    /**
     * Simple EngineResultStream backed by a list of batches.
     */
    private static class TestStream implements EngineResultStream {
        private final List<EngineResultBatch> batches;

        TestStream(List<EngineResultBatch> batches) {
            this.batches = batches;
        }

        @Override
        public Iterator<EngineResultBatch> iterator() {
            return batches.iterator();
        }

        @Override
        public void close() {}
    }

    // ── 12.4 testExecuteFragmentStreamingSendsBatchesViaChannel ──

    public void testExecuteFragmentStreamingSendsBatchesViaChannel() throws Exception {
        VectorSchemaRoot vsr1 = mock(VectorSchemaRoot.class);
        VectorSchemaRoot vsr2 = mock(VectorSchemaRoot.class);

        EngineResultBatch batch1 = mock(EngineResultBatch.class);
        when(batch1.getArrowRoot()).thenReturn(vsr1);
        EngineResultBatch batch2 = mock(EngineResultBatch.class);
        when(batch2.getArrowRoot()).thenReturn(vsr2);

        TestStream stream = new TestStream(List.of(batch1, batch2));

        AnalyticsSearchBackendPlugin backend = mockBackend(stream);
        AnalyticsSearchService service = new AnalyticsSearchService(Map.of(BACKEND_ID, backend));

        IndexShard shard = mockShard(null);
        TransportChannel channel = mock(TransportChannel.class);
        AnalyticsShardTask task = mock(AnalyticsShardTask.class);

        service.executeFragmentStreaming(createRequest(), shard, task, channel);

        ArgumentCaptor<FragmentExecutionResponse> captor = ArgumentCaptor.forClass(FragmentExecutionResponse.class);
        verify(channel, times(2)).sendResponseBatch(captor.capture());
        verify(channel, times(1)).completeStream();

        List<FragmentExecutionResponse> responses = captor.getAllValues();
        assertSame(vsr1, responses.get(0).getRoot());
        assertSame(vsr2, responses.get(1).getRoot());
    }


}
