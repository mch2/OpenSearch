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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link RowBatchToArrowConverter}.
 *
 * Validates: Requirements 5.1, 5.2, 5.3, 5.5
 */
public class RowBatchToArrowConverterTests extends OpenSearchTestCase {

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

    // ---- 12.1: Long, Double, String, Boolean → VSR with correct vectors ----

    public void testConvertPrimitiveTypes() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field(
                    "score",
                    FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                    null
                ),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
                new Field("active", FieldType.nullable(ArrowType.Bool.INSTANCE), null)
            )
        );

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 1L, 3.14, "alice", true });
        rows.add(new Object[] { 2L, 2.71, "bob", false });

        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "score", "name", "active"), rows);

        try (VectorSchemaRoot vsr = RowBatchToArrowConverter.convert(response, schema, allocator)) {
            assertEquals(2, vsr.getRowCount());

            BigIntVector idVec = (BigIntVector) vsr.getVector("id");
            assertEquals(1L, idVec.get(0));
            assertEquals(2L, idVec.get(1));

            Float8Vector scoreVec = (Float8Vector) vsr.getVector("score");
            assertEquals(3.14, scoreVec.get(0), 0.001);
            assertEquals(2.71, scoreVec.get(1), 0.001);

            VarCharVector nameVec = (VarCharVector) vsr.getVector("name");
            assertEquals("alice", new String(nameVec.get(0)));
            assertEquals("bob", new String(nameVec.get(1)));

            BitVector activeVec = (BitVector) vsr.getVector("active");
            assertEquals(1, activeVec.get(0));
            assertEquals(0, activeVec.get(1));
        }
    }

    // ---- 12.2: null cells set validity bits correctly ----

    public void testConvertWithNulls() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 1L, null });
        rows.add(new Object[] { null, "bob" });

        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "name"), rows);

        try (VectorSchemaRoot vsr = RowBatchToArrowConverter.convert(response, schema, allocator)) {
            assertEquals(2, vsr.getRowCount());

            BigIntVector idVec = (BigIntVector) vsr.getVector("id");
            assertTrue("Row 0 id should be set", idVec.isNull(0) == false);
            assertEquals(1L, idVec.get(0));
            assertTrue("Row 1 id should be null", idVec.isNull(1));

            VarCharVector nameVec = (VarCharVector) vsr.getVector("name");
            assertTrue("Row 0 name should be null", nameVec.isNull(0));
            assertTrue("Row 1 name should be set", nameVec.isNull(1) == false);
            assertEquals("bob", new String(nameVec.get(1)));
        }
    }

    // ---- 12.3: row value type doesn't match column type → IllegalArgumentException ----

    public void testConvertMismatchedSchemaThrows() {
        Schema schema = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { "not_a_long" });

        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id"), rows);

        expectThrows(IllegalArgumentException.class, () -> {
            try (VectorSchemaRoot vsr = RowBatchToArrowConverter.convert(response, schema, allocator)) {
                // should not reach here
            }
        });
    }

    // ---- 12.4: zero-row response → empty VSR with schema preserved ----

    public void testConvertEmptyRows() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );

        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "name"), new ArrayList<>());

        try (VectorSchemaRoot vsr = RowBatchToArrowConverter.convert(response, schema, allocator)) {
            assertEquals(0, vsr.getRowCount());
            assertEquals(2, vsr.getSchema().getFields().size());
            assertEquals("id", vsr.getSchema().getFields().get(0).getName());
            assertEquals("name", vsr.getSchema().getFields().get(1).getName());
        }
    }

    // ---- 12.5: Arrow Text / generic CharSequence coerced to String ----

    public void testConvertStringAsCharSequence() {
        Schema schema = new Schema(List.of(new Field("label", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

        // Simulate a CharSequence value (StringBuilder) that should be coerced to String
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { new StringBuilder("hello") });

        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("label"), rows);

        try (VectorSchemaRoot vsr = RowBatchToArrowConverter.convert(response, schema, allocator)) {
            assertEquals(1, vsr.getRowCount());
            VarCharVector vec = (VarCharVector) vsr.getVector("label");
            assertEquals("hello", new String(vec.get(0)));
        }
    }
}
