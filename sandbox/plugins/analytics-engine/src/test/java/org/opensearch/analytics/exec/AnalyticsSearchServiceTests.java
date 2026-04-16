/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ScanResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AnalyticsSearchService}.
 */
public class AnalyticsSearchServiceTests extends OpenSearchTestCase {

    /**
     * Mock {@link EngineResultBatch} that accepts configurable field names and row data.
     * Each row is an Object array; field values are looked up by column index matching the field name.
     */
    static class MockResultBatch implements EngineResultBatch {
        private final List<String> fieldNames;
        private final List<Object[]> rows;

        MockResultBatch(List<String> fieldNames, List<Object[]> rows) {
            this.fieldNames = fieldNames;
            this.rows = rows;
        }

        @Override
        public List<String> getFieldNames() {
            return fieldNames;
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            int colIndex = fieldNames.indexOf(fieldName);
            if (colIndex < 0 || rowIndex < 0 || rowIndex >= rows.size()) {
                return null;
            }
            return rows.get(rowIndex)[colIndex];
        }
    }

    /**
     * Mock {@link EngineResultStream} that returns a sequence of pre-built batches.
     */
    static class MockResultStream implements EngineResultStream {
        private final List<EngineResultBatch> batches;

        MockResultStream(List<EngineResultBatch> batches) {
            this.batches = batches;
        }

        @Override
        public Iterator<EngineResultBatch> iterator() {
            return batches.iterator();
        }

        @Override
        public void close() {}
    }

    private AnalyticsSearchService createService() {
        return new AnalyticsSearchService(Map.of());
    }

    public void testCollectResponseSingleBatch() {
        AnalyticsSearchService service = createService();

        List<String> fieldNames = List.of("name", "score");
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "alice", 95 });
        rows.add(new Object[] { "bob", 87 });
        rows.add(new Object[] { "carol", 72 });

        MockResultBatch batch = new MockResultBatch(fieldNames, rows);
        MockResultStream stream = new MockResultStream(List.of(batch));

        ScanResponse response = service.collectResponse(stream);

        assertEquals(List.of("name", "score"), response.getFieldNames());
        assertEquals(3, response.getRows().size());
        assertArrayEquals(new Object[] { "alice", 95 }, response.getRows().get(0));
        assertArrayEquals(new Object[] { "bob", 87 }, response.getRows().get(1));
        assertArrayEquals(new Object[] { "carol", 72 }, response.getRows().get(2));
    }

    public void testCollectResponseMultipleBatches() {
        AnalyticsSearchService service = createService();

        List<String> fieldNames = List.of("id", "value");

        List<Object[]> batch1Rows = new ArrayList<>();
        batch1Rows.add(new Object[] { 1, "a" });
        batch1Rows.add(new Object[] { 2, "b" });
        MockResultBatch batch1 = new MockResultBatch(fieldNames, batch1Rows);

        List<Object[]> batch2Rows = new ArrayList<>();
        batch2Rows.add(new Object[] { 3, "c" });
        MockResultBatch batch2 = new MockResultBatch(fieldNames, batch2Rows);

        MockResultStream stream = new MockResultStream(List.of(batch1, batch2));

        ScanResponse response = service.collectResponse(stream);

        // Field names come from the first batch
        assertEquals(List.of("id", "value"), response.getFieldNames());
        // Rows from both batches are concatenated
        assertEquals(3, response.getRows().size());
        assertArrayEquals(new Object[] { 1, "a" }, response.getRows().get(0));
        assertArrayEquals(new Object[] { 2, "b" }, response.getRows().get(1));
        assertArrayEquals(new Object[] { 3, "c" }, response.getRows().get(2));
    }

    public void testCollectResponseEmptyStream() {
        AnalyticsSearchService service = createService();

        MockResultStream stream = new MockResultStream(Collections.emptyList());

        ScanResponse response = service.collectResponse(stream);

        assertEquals(List.of(), response.getFieldNames());
        assertEquals(0, response.getRows().size());
    }

    public void testExecuteFragmentThrowsWhenNoCompositeEngine() {
        AnalyticsSearchService service = createService();

        IndexShard mockShard = mock(IndexShard.class);
        when(mockShard.getCompositeEngine()).thenReturn(null);
        when(mockShard.shardId()).thenReturn(new ShardId(new Index("test_index", "_na_"), 0));

        FragmentExecutionRequest request = new FragmentExecutionRequest(
            "query-1",
            0,
            new ShardId(new Index("test_index", "_na_"), 0),
            List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null))
        );

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> service.executeFragment(request, mockShard));
        assertTrue(ex.getMessage().contains("No CompositeEngine"));
    }
}
