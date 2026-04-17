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
import org.opensearch.be.datafusion.internal.InputHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
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
     * feed() with a 2-row VectorSchemaRoot pushes the batch to the handle.
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

        // Build a VectorSchemaRoot with 2 rows
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        vsr.allocateNew();
        BigIntVector idVec = (BigIntVector) vsr.getVector("id");
        idVec.set(0, 10L);
        idVec.set(1, 20L);
        idVec.setValueCount(2);
        VarCharVector nameVec = (VarCharVector) vsr.getVector("name");
        nameVec.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
        nameVec.setSafe(1, "bob".getBytes(StandardCharsets.UTF_8));
        nameVec.setValueCount(2);
        vsr.setRowCount(2);

        sink.feed(vsr);

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
        VectorSchemaRoot emptyVsr = VectorSchemaRoot.create(schema, allocator);
        emptyVsr.allocateNew();
        emptyVsr.setRowCount(0);
        emptyVsr.getVector("id").setValueCount(0);
        emptyVsr.getVector("name").setValueCount(0);
        sink.feed(emptyVsr);

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

        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        vsr.allocateNew();
        BigIntVector idVec = (BigIntVector) vsr.getVector("id");
        idVec.set(0, 1L);
        idVec.setValueCount(1);
        VarCharVector nameVec = (VarCharVector) vsr.getVector("name");
        nameVec.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
        nameVec.setValueCount(1);
        vsr.setRowCount(1);

        expectThrows(IllegalStateException.class, () -> sink.feed(vsr));
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

    // ---- batchToVsr tests ----

    /**
     * batchToVsr converts an EngineResultBatch to a VectorSchemaRoot.
     */
    public void testBatchToVsrConvertsCorrectly() {
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

        VectorSchemaRoot vsr = DatafusionLocalStageContext.batchToVsr(batch, allocator);

        assertEquals(2, vsr.getSchema().getFields().size());
        assertEquals("age", vsr.getSchema().getFields().get(0).getName());
        assertEquals("city", vsr.getSchema().getFields().get(1).getName());
        assertEquals(2, vsr.getRowCount());
        // Values are stored as VarChar (strings)
        assertEquals("25", vsr.getVector("age").getObject(0).toString());
        assertEquals("NYC", vsr.getVector("city").getObject(0).toString());
        assertEquals("30", vsr.getVector("age").getObject(1).toString());
        assertEquals("LA", vsr.getVector("city").getObject(1).toString());

        vsr.close();
    }

    /**
     * batchToVsr with zero rows returns an empty VectorSchemaRoot.
     */
    public void testBatchToVsrEmptyBatch() {
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

        VectorSchemaRoot vsr = DatafusionLocalStageContext.batchToVsr(batch, allocator);
        assertEquals(1, vsr.getSchema().getFields().size());
        assertEquals("x", vsr.getSchema().getFields().get(0).getName());
        assertEquals(0, vsr.getRowCount());

        vsr.close();
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
