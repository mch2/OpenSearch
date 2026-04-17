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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link FragmentExecutionResponse} implementing {@code ArrowBatchResponse}.
 *
 * Validates: Requirements 1.3, 1.4
 */
public class FragmentExecutionResponseArrowTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testGetArrowRootReturnsConstructedVSR() {
        Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FragmentExecutionResponse response = new FragmentExecutionResponse(root);
            assertSame(root, response.getRoot());
        }
    }

    public void testGetArrowSchemaReturnsRootSchema() {
        Schema schema = new Schema(
            List.of(
                Field.nullable("id", new ArrowType.Int(64, true)),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FragmentExecutionResponse response = new FragmentExecutionResponse(root);
            assertEquals(root.getSchema(), response.getRoot().getSchema());
        }
    }

    public void testWriteToThrowsUnsupported() {
        Schema schema = new Schema(List.of(Field.nullable("x", new ArrowType.Int(32, true))));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FragmentExecutionResponse response = new FragmentExecutionResponse(root);
            BytesStreamOutput out = new BytesStreamOutput();
            expectThrows(UnsupportedOperationException.class, () -> response.writeTo(out));
        }
    }

    public void testStreamInputConstructorThrowsUnsupported() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeString("dummy");
        expectThrows(UnsupportedOperationException.class, () -> new FragmentExecutionResponse(out.bytes().streamInput()));
    }
}
