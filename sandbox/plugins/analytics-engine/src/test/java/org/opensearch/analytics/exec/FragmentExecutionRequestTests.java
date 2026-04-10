/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for {@link FragmentExecutionRequest} serialization and getters.
 */
public class FragmentExecutionRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String queryId = "query-123";
        int stageId = 7;
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        List<FragmentExecutionRequest.PlanAlternative> alternatives = List.of(
            new FragmentExecutionRequest.PlanAlternative("lucene", "plan-bytes".getBytes(StandardCharsets.UTF_8)),
            new FragmentExecutionRequest.PlanAlternative("datafusion", "substrait-bytes".getBytes(StandardCharsets.UTF_8))
        );

        FragmentExecutionRequest original = new FragmentExecutionRequest(queryId, stageId, shardId, alternatives);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionRequest deserialized = new FragmentExecutionRequest(in);

        assertEquals(queryId, deserialized.getQueryId());
        assertEquals(stageId, deserialized.getStageId());
        assertEquals(shardId, deserialized.getShardId());
        assertEquals(2, deserialized.getPlanAlternatives().size());
        assertEquals("lucene", deserialized.getPlanAlternatives().get(0).getBackendId());
        assertArrayEquals("plan-bytes".getBytes(StandardCharsets.UTF_8), deserialized.getPlanAlternatives().get(0).getFragmentBytes());
        assertEquals("datafusion", deserialized.getPlanAlternatives().get(1).getBackendId());
        assertArrayEquals("substrait-bytes".getBytes(StandardCharsets.UTF_8), deserialized.getPlanAlternatives().get(1).getFragmentBytes());
    }

    public void testNullFragmentBytesRoundTrip() throws IOException {
        ShardId shardId = new ShardId(new Index("test-index", "uuid"), 0);
        List<FragmentExecutionRequest.PlanAlternative> alternatives = List.of(new FragmentExecutionRequest.PlanAlternative("lucene", null));

        FragmentExecutionRequest original = new FragmentExecutionRequest("query-456", 2, shardId, alternatives);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        FragmentExecutionRequest deserialized = new FragmentExecutionRequest(in);

        assertEquals(1, deserialized.getPlanAlternatives().size());
        assertNull("fragmentBytes should be null after round-trip", deserialized.getPlanAlternatives().get(0).getFragmentBytes());
        assertEquals("lucene", deserialized.getPlanAlternatives().get(0).getBackendId());
        assertEquals("query-456", deserialized.getQueryId());
        assertEquals(2, deserialized.getStageId());
    }

    public void testGetters() {
        String queryId = "query-789";
        int stageId = 3;
        ShardId shardId = new ShardId(new Index("my-index", "my-uuid"), 5);
        byte[] fragmentBytes = new byte[] { 1, 2, 3 };
        List<FragmentExecutionRequest.PlanAlternative> alternatives = List.of(
            new FragmentExecutionRequest.PlanAlternative("lucene", fragmentBytes)
        );

        FragmentExecutionRequest request = new FragmentExecutionRequest(queryId, stageId, shardId, alternatives);

        assertEquals(queryId, request.getQueryId());
        assertEquals(stageId, request.getStageId());
        assertEquals(shardId, request.getShardId());
        assertEquals(1, request.getPlanAlternatives().size());
        assertEquals("lucene", request.getPlanAlternatives().get(0).getBackendId());
        assertArrayEquals(fragmentBytes, request.getPlanAlternatives().get(0).getFragmentBytes());
    }
}
