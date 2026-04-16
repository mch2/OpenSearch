/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.be.datafusion.internal.InputHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link DatafusionLocalStageContext} helpers, {@link DatafusionChildSink},
 * and the backend-local copy of {@link ArrowSchemaFromCalcite}.
 * <p>
 * The full {@link DatafusionLocalStageContext} lifecycle requires native libraries
 * and is covered by the integration test. These unit tests exercise the components
 * that can be tested in isolation.
 */
public class DatafusionLocalStageContextTests extends OpenSearchTestCase {

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

    // ---- DatafusionChildSink tests ----

    /**
     * feed() with a 2-row response converts to Arrow and pushes to the handle.
     */
    public void testChildSinkFeedPushesBatchToHandle() {
        Schema schema = twoColumnSchema();
        AtomicReference<VectorSchemaRoot> captured = new AtomicReference<>();

        InputHandle handle = new InputHandle() {
            @Override
            public void pushBatch(VectorSchemaRoot batch) {
                assertEquals(2, batch.getRowCount());
                BigIntVector idVec = (BigIntVector) batch.getVector("id");
                assertEquals(10L, idVec.get(0));
                assertEquals(20L, idVec.get(1));
                VarCharVector nameVec = (VarCharVector) batch.getVector("name");
                assertEquals("alice", new String(nameVec.get(0)));
                assertEquals("bob", new String(nameVec.get(1)));
                captured.set(batch);
            }

            @Override
            public void closeInput() {}
        };

        DatafusionChildSink sink = new DatafusionChildSink(handle, schema, allocator, 42);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 10L, "alice" });
        rows.add(new Object[] { 20L, "bob" });
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "name"), rows);

        sink.feed(response);

        assertNotNull("pushBatch should have been called", captured.get());
        captured.get().close();
    }

    /**
     * feed() with zero rows pushes an empty VSR.
     */
    public void testChildSinkFeedWithZeroRows() {
        Schema schema = twoColumnSchema();
        AtomicReference<VectorSchemaRoot> captured = new AtomicReference<>();

        InputHandle handle = new InputHandle() {
            @Override
            public void pushBatch(VectorSchemaRoot batch) {
                assertEquals(0, batch.getRowCount());
                captured.set(batch);
            }

            @Override
            public void closeInput() {}
        };

        DatafusionChildSink sink = new DatafusionChildSink(handle, schema, allocator, 7);
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "name"), new ArrayList<>());
        sink.feed(response);

        assertNotNull("pushBatch should have been called for empty batch", captured.get());
        captured.get().close();
    }

    /**
     * feed() after the handle has been closed throws.
     */
    public void testChildSinkFeedAfterHandleCloseThrows() {
        Schema schema = twoColumnSchema();
        AtomicBoolean closed = new AtomicBoolean(false);

        InputHandle handle = new InputHandle() {
            @Override
            public void pushBatch(VectorSchemaRoot batch) {
                if (closed.get()) {
                    batch.close();
                    throw new IllegalStateException("pushBatch after closeInput");
                }
                batch.close();
            }

            @Override
            public void closeInput() {
                closed.set(true);
            }
        };

        DatafusionChildSink sink = new DatafusionChildSink(handle, schema, allocator, 3);
        handle.closeInput();

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] { 1L, "test" });
        FragmentExecutionResponse response = new FragmentExecutionResponse(List.of("id", "name"), rows);

        expectThrows(IllegalStateException.class, () -> sink.feed(response));
    }

    /**
     * close() on DatafusionChildSink is a no-op (does not close the handle).
     */
    public void testChildSinkCloseIsNoOp() {
        AtomicBoolean handleClosed = new AtomicBoolean(false);
        InputHandle handle = new InputHandle() {
            @Override
            public void pushBatch(VectorSchemaRoot batch) {}

            @Override
            public void closeInput() {
                handleClosed.set(true);
            }
        };

        DatafusionChildSink sink = new DatafusionChildSink(handle, twoColumnSchema(), allocator, 1);
        sink.close();
        assertFalse("close() on DatafusionChildSink should NOT close the InputHandle", handleClosed.get());
    }

    /**
     * readResult returns empty, getRowCount returns 0, getValueAt returns null.
     */
    public void testChildSinkReadMethodsReturnDefaults() {
        InputHandle handle = new InputHandle() {
            @Override
            public void pushBatch(VectorSchemaRoot batch) {}

            @Override
            public void closeInput() {}
        };

        DatafusionChildSink sink = new DatafusionChildSink(handle, twoColumnSchema(), allocator, 1);
        assertFalse("readResult should be empty", sink.readResult().iterator().hasNext());
        assertEquals(0, sink.getRowCount());
        assertNull(sink.getValueAt("id", 0));
    }

    // ---- batchToResponse tests ----

    /**
     * batchToResponse converts an EngineResultBatch to a FragmentExecutionResponse.
     */
    public void testBatchToResponseConvertsCorrectly() {
        EngineResultBatch batch = new EngineResultBatch() {
            @Override
            public List<String> getFieldNames() {
                return List.of("age", "city");
            }

            @Override
            public int getRowCount() {
                return 2;
            }

            @Override
            public Object getFieldValue(String fieldName, int rowIndex) {
                if ("age".equals(fieldName)) {
                    return rowIndex == 0 ? 25L : 30L;
                }
                return rowIndex == 0 ? "NYC" : "LA";
            }
        };

        FragmentExecutionResponse response = DatafusionLocalStageContext.batchToResponse(batch);

        assertEquals(List.of("age", "city"), response.getFieldNames());
        assertEquals(2, response.getRows().size());
        assertEquals(25L, response.getRows().get(0)[0]);
        assertEquals("NYC", response.getRows().get(0)[1]);
        assertEquals(30L, response.getRows().get(1)[0]);
        assertEquals("LA", response.getRows().get(1)[1]);
    }

    /**
     * batchToResponse with zero rows returns an empty response.
     */
    public void testBatchToResponseEmptyBatch() {
        EngineResultBatch batch = new EngineResultBatch() {
            @Override
            public List<String> getFieldNames() {
                return List.of("x");
            }

            @Override
            public int getRowCount() {
                return 0;
            }

            @Override
            public Object getFieldValue(String fieldName, int rowIndex) {
                throw new IndexOutOfBoundsException();
            }
        };

        FragmentExecutionResponse response = DatafusionLocalStageContext.batchToResponse(batch);
        assertEquals(List.of("x"), response.getFieldNames());
        assertTrue(response.getRows().isEmpty());
    }

    // ---- stageInputId tests ----

    public void testStageInputIdFormat() {
        assertEquals("__stage_0_input__", DatafusionLocalStageContext.stageInputId(0));
        assertEquals("__stage_42_input__", DatafusionLocalStageContext.stageInputId(42));
    }

    // ---- ArrowSchemaFromCalcite (backend-local copy) tests ----

    public void testArrowSchemaFromCalciteBigintDoubleVarchar() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.builder()
            .add("id", SqlTypeName.BIGINT)
            .add("score", SqlTypeName.DOUBLE)
            .add("name", SqlTypeName.VARCHAR)
            .build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        assertEquals(3, schema.getFields().size());
        assertEquals("id", schema.getFields().get(0).getName());
        assertEquals(new ArrowType.Int(64, true), schema.getFields().get(0).getType());
        assertEquals("score", schema.getFields().get(1).getName());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), schema.getFields().get(1).getType());
        assertEquals("name", schema.getFields().get(2).getName());
        assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(2).getType());
    }

    public void testArrowSchemaFromCalciteAllFieldsNullable() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.builder().add("id", SqlTypeName.BIGINT).add("name", SqlTypeName.VARCHAR).build();

        Schema schema = ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);
        for (Field field : schema.getFields()) {
            assertTrue("Field '" + field.getName() + "' should be nullable", field.isNullable());
        }
    }

    public void testArrowSchemaFromCalciteUnsupportedTypeThrows() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.builder().add("ts", SqlTypeName.TIMESTAMP).build();
        expectThrows(IllegalArgumentException.class, () -> ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType));
    }

    // ---- Helpers ----

    private static Schema twoColumnSchema() {
        return new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );
    }
}
