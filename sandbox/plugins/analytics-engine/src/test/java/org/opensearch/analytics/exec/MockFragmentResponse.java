/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Test utility that builds a {@link FragmentExecutionResponse} from field names
 * and row data, replacing the deleted {@code ScanResponse(List, List)} constructor.
 * All columns are treated as VARCHAR for test simplicity.
 */
public final class MockFragmentResponse {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private MockFragmentResponse() {}

    /**
     * Build a {@link FragmentExecutionResponse} wrapping a real {@link VectorSchemaRoot}
     * with VARCHAR columns populated from the given rows.
     */
    public static FragmentExecutionResponse create(List<String> fieldNames, List<Object[]> rows) {
        List<Field> fields = new ArrayList<>();
        for (String name : fieldNames) {
            fields.add(new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        }
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOCATOR);
        root.setRowCount(rows.size());
        for (int col = 0; col < fieldNames.size(); col++) {
            VarCharVector vec = (VarCharVector) root.getVector(col);
            vec.allocateNew();
            for (int row = 0; row < rows.size(); row++) {
                Object val = rows.get(row)[col];
                if (val != null) {
                    vec.setSafe(row, String.valueOf(val).getBytes(StandardCharsets.UTF_8));
                }
            }
            vec.setValueCount(rows.size());
        }
        return new FragmentExecutionResponse(root);
    }
}
