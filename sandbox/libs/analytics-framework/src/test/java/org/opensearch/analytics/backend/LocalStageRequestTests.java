/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link LocalStageRequest}.
 */
public class LocalStageRequestTests extends OpenSearchTestCase {

    public void testGettersAndImmutability() {
        String queryId = "test-query-123";
        int stageId = 7;
        byte[] fragmentBytes = new byte[] { 1, 2, 3, 4 };
        BufferAllocator allocator = mock(BufferAllocator.class);
        ExchangeSink downstream = mock(ExchangeSink.class);

        Schema schema = new Schema(List.of(Field.nullable("col1", new ArrowType.Int(32, true))));
        Map<Integer, Schema> childSchemas = new HashMap<>();
        childSchemas.put(1, schema);
        childSchemas.put(2, schema);

        LocalStageRequest req = new LocalStageRequest(queryId, stageId, fragmentBytes, allocator, downstream, childSchemas);

        // Verify getters return correct values
        assertEquals("test-query-123", req.getQueryId());
        assertEquals(7, req.getStageId());
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, req.getFragmentBytes());
        assertSame(allocator, req.getAllocator());
        assertSame(downstream, req.getDownstream());
        assertEquals(2, req.getChildSchemas().size());
        assertSame(schema, req.getChildSchemas().get(1));
        assertSame(schema, req.getChildSchemas().get(2));

        // Verify immutability: modifying the original byte array doesn't affect the request
        fragmentBytes[0] = 99;
        assertEquals(1, req.getFragmentBytes()[0]);

        // Verify immutability: getFragmentBytes returns a defensive copy each time
        byte[] copy1 = req.getFragmentBytes();
        byte[] copy2 = req.getFragmentBytes();
        assertNotSame(copy1, copy2);
        assertArrayEquals(copy1, copy2);

        // Verify immutability: modifying the original map doesn't affect the request
        childSchemas.put(99, schema);
        assertEquals(2, req.getChildSchemas().size());
        assertNull(req.getChildSchemas().get(99));

        // Verify immutability: the returned map is unmodifiable
        expectThrows(UnsupportedOperationException.class, () -> req.getChildSchemas().put(99, schema));
    }
}
