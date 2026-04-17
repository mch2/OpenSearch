/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link FragmentExecutionResponse} focused on construction and
 * basic accessor semantics. Full wire-level serialization is covered by the
 * streaming IT path ({@code AnalyticsShardDispatchIT}) because the receive-side
 * constructor relies on Flight-internal {@code VectorStreamInput} which only
 * lives inside the Flight-RPC plugin's package scope.
 */
public class FragmentExecutionResponseTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        if (allocator != null) allocator.close();
        super.tearDown();
    }

    public void testExtendsArrowBatchResponse() {
        // Design assertion — FragmentExecutionResponse must ride the native Arrow
        // transport path (zero-copy transfer via ArrowBatchResponse) rather than
        // falling through to the byte-serialization path.
        assertTrue(
            "FragmentExecutionResponse must extend ArrowBatchResponse",
            ArrowBatchResponse.class.isAssignableFrom(FragmentExecutionResponse.class)
        );
    }

    public void testConstructorStoresRootAndIsAccessible() {
        try (VectorSchemaRoot root = newIntRoot("x", 3)) {
            FragmentExecutionResponse response = new FragmentExecutionResponse(root);
            assertSame("getArrowRoot returns the producer root", root, response.getArrowRoot());
            assertSame("getRoot (ArrowBatchResponse) matches", root, response.getRoot());
        }
    }

    public void testArrowRootPreservesRowCount() {
        try (VectorSchemaRoot root = newIntRoot("x", 5)) {
            FragmentExecutionResponse response = new FragmentExecutionResponse(root);
            assertEquals("row count surfaces via getArrowRoot", 5, response.getArrowRoot().getRowCount());
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private VectorSchemaRoot newIntRoot(String fieldName, int rowCount) {
        Field field = new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector vec = (IntVector) root.getVector(fieldName);
        vec.allocateNew();
        for (int i = 0; i < rowCount; i++) {
            vec.setSafe(i, i);
        }
        vec.setValueCount(rowCount);
        root.setRowCount(rowCount);
        return root;
    }
}
