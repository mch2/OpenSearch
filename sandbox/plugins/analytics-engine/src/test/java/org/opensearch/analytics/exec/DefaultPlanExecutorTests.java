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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.opensearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;

/**
 * Tests for {@link DefaultPlanExecutor}'s row-materialization boundary.
 *
 * <p>The end-to-end {@code execute(RelNode, Object)} path involves Guice-wired
 * dependencies (TransportService, Scheduler, TaskManager, CapabilityRegistry,
 * EngineContextProvider, NodeClient) and is exercised by internal cluster tests.
 * These unit tests cover the one deterministic piece of behavior that lives
 * in this class: batches-to-rows conversion at the external API edge.
 */
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

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

    public void testBatchesToRowsEmpty() {
        Iterable<Object[]> rows = DefaultPlanExecutor.batchesToRows(List.of());
        assertFalse("no batches → no rows", rows.iterator().hasNext());
    }

    public void testBatchesToRowsSingleBatchIntegers() {
        VectorSchemaRoot batch = makeIntBatch("x", 10, 20, 30);
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(3, rows.size());
        assertArrayEquals(new Object[] { 10 }, rows.get(0));
        assertArrayEquals(new Object[] { 20 }, rows.get(1));
        assertArrayEquals(new Object[] { 30 }, rows.get(2));
    }

    public void testBatchesToRowsMultipleBatchesPreservesOrder() {
        VectorSchemaRoot batch1 = makeIntBatch("x", 1, 2);
        VectorSchemaRoot batch2 = makeIntBatch("x", 3);
        VectorSchemaRoot batch3 = makeIntBatch("x", 4, 5);
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch1, batch2, batch3)));
        assertEquals(5, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals(3, rows.get(2)[0]);
        assertEquals(4, rows.get(3)[0]);
        assertEquals(5, rows.get(4)[0]);
    }

    public void testBatchesToRowsMultipleColumns() {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
            )
        );
        VectorSchemaRoot batch = VectorSchemaRoot.create(schema, allocator);
        batch.allocateNew();
        BigIntVector ids = (BigIntVector) batch.getVector(0);
        VarCharVector names = (VarCharVector) batch.getVector(1);
        ids.setSafe(0, 100L);
        ids.setSafe(1, 200L);
        names.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
        names.setSafe(1, "bob".getBytes(StandardCharsets.UTF_8));
        batch.setRowCount(2);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(2, rows.size());
        assertEquals(100L, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(200L, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);
    }

    public void testBatchesToRowsHandlesNulls() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null))),
            allocator
        );
        batch.allocateNew();
        IntVector vec = (IntVector) batch.getVector(0);
        vec.setSafe(0, 1);
        vec.setNull(1);
        vec.setSafe(2, 3);
        batch.setRowCount(3);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertNull(rows.get(1)[0]);
        assertEquals(3, rows.get(2)[0]);
    }

    public void testBatchesToRowsVarCharDecodedAsString() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(
            new Schema(List.of(new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))),
            allocator
        );
        batch.allocateNew();
        VarCharVector vec = (VarCharVector) batch.getVector(0);
        vec.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
        vec.setSafe(1, "world".getBytes(StandardCharsets.UTF_8));
        batch.setRowCount(2);

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch)));
        assertEquals("hello", rows.get(0)[0]);
        assertEquals("world", rows.get(1)[0]);
        assertTrue(rows.get(0)[0] instanceof String);
    }

    public void testBatchesToRowsClosesBatches() {
        BufferAllocator child = allocator.newChildAllocator("test", 0, Long.MAX_VALUE);
        VectorSchemaRoot batch = makeIntBatch(child, "x", 1, 2);
        long before = child.getAllocatedMemory();
        assertTrue("batch should hold allocated memory", before > 0);
        DefaultPlanExecutor.batchesToRows(List.of(batch));
        assertEquals("batch buffers should be released after batchesToRows", 0, child.getAllocatedMemory());
        child.close();
    }

    /**
     * The coordinator's Arrow batch can present columns in physical/scan order (e.g. a
     * no-projection scan over a dynamically-mapped index comes back alphabetically [age, name]).
     * batchesToRows must reorder to the plan's declared column order [name, age] so a positional
     * consumer (SQL frontend) names each value correctly. Without this, name/age (or name/alias)
     * values transpose.
     */
    public void testBatchesToRowsReordersToTargetColumnOrder() {
        VectorSchemaRoot batch = makeAgeNameBatch(20L, "hello");  // physical order: [age, name]
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch), List.of("name", "age")));
        assertEquals(1, rows.size());
        assertArrayEquals("columns must be reordered to [name, age]", new Object[] { "hello", 20L }, rows.get(0));
    }

    /**
     * Contract: an unknown target column name is a planner/executor invariant violation —
     * {@code orderedColumns} throws rather than dropping the column or substituting null.
     * Silent fallback would let a misaligned plan return wrong-but-shape-valid rows, which
     * is harder to diagnose than a fast failure. If the upstream caller wants tolerance,
     * it must filter target names before invoking {@code batchesToRows}.
     */
    public void testBatchesToRowsThrowsWhenTargetNameMissing() {
        VectorSchemaRoot batch = makeAgeNameBatch(20L, "hello");  // [age, name]
        expectThrows(IllegalStateException.class, () -> DefaultPlanExecutor.batchesToRows(List.of(batch), List.of("name", "nonexistent")));
    }

    // ── upfront charge → shrinkTo-actual on materialization ───────────────

    /**
     * Charging is UPFRONT (worst-case) in executeInternal, not per-batch here. After materializing,
     * batchesToRows calls {@link ResultHeapCharge#shrinkTo} with the actual native size, releasing the
     * over-reservation. This asserts the reservation shrinks from a pessimistic pre-charge down toward
     * the true footprint.
     */
    public void testBatchesToRowsShrinksChargeToActual() {
        CircuitBreaker breaker = requestBreaker(10_000_000);
        ResultHeapCharge charge = new ResultHeapCharge(breaker, "q-shrink", 1.0);
        // Pre-charge a pessimistic worst case, mirroring executeInternal's upfront admission.
        charge.charge(1_000_000);
        assertEquals(1_000_000, breaker.getUsed());

        VectorSchemaRoot batch = makeIntBatch("x", 1, 2, 3);
        long actualNative = 0;
        for (org.apache.arrow.vector.FieldVector v : batch.getFieldVectors()) {
            actualNative += v.getBufferSize();
        }

        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch), null, charge));
        assertEquals(3, rows.size());
        assertTrue("reservation must shrink below the pessimistic pre-charge", charge.chargedBytes() < 1_000_000);
        assertEquals("reservation must shrink to the actual native footprint", actualNative, charge.chargedBytes());
        assertEquals(actualNative, breaker.getUsed());
        charge.close();
        assertEquals("close returns the shared breaker to zero", 0, breaker.getUsed());
    }

    /**
     * Every batch's native buffer is released even when materialization throws mid-drain — the batches
     * are owned by a Releasables.wrap cleanup in try-with-resources, so a throw (here: a missing target
     * column while ordering the first batch) still closes all batches, including ones never reached.
     * This is the leak the previous {@code catch (RuntimeException)} drain missed on Error paths.
     */
    public void testBatchesToRowsClosesAllBatchesWhenMaterializationThrows() {
        BufferAllocator child = allocator.newChildAllocator("throw", 0, Long.MAX_VALUE);
        VectorSchemaRoot b1 = makeIntBatch(child, "x", 1, 2);
        VectorSchemaRoot b2 = makeIntBatch(child, "x", 3, 4);
        assertTrue(child.getAllocatedMemory() > 0);

        // Target column "nonexistent" is absent from every batch → orderedColumns throws on batch 0;
        // batch 1 is never materialized but must still be closed by the wrapper.
        expectThrows(IllegalStateException.class, () -> DefaultPlanExecutor.batchesToRows(List.of(b1, b2), List.of("nonexistent"), null));
        assertEquals("all batches must be closed even when materialization throws", 0, child.getAllocatedMemory());
        child.close();
    }

    /** A null heapCharge (no breaker registered / tests) leaves materialization unaccounted, no NPE. */
    public void testBatchesToRowsNullChargeIsUnaccounted() {
        VectorSchemaRoot batch = makeIntBatch("x", 7, 8);
        List<Object[]> rows = toList(DefaultPlanExecutor.batchesToRows(List.of(batch), null, null));
        assertEquals(2, rows.size());
    }

    // ── worst-case admission estimate ─────────────────────────────────────

    /**
     * estimateWorstCaseResultBytes multiplies the per-column widths by the row cap. Fixed-width
     * columns use the exact Arrow buffer width + 1 validity byte; variable-width columns use the
     * allowance + 4-byte offset + 1 validity byte.
     */
    public void testEstimateWorstCaseResultBytes() {
        // BIGINT (8+1) + VARCHAR (allowance 256 + 4 + 1) = 9 + 261 = 270 per row.
        org.apache.calcite.rel.type.RelDataTypeFactory factory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT
        );
        org.apache.calcite.rel.type.RelDataType rowType = factory.builder()
            .add("id", org.apache.calcite.sql.type.SqlTypeName.BIGINT)
            .add("name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
            .build();

        long perRow = (8 + 1) + (256 + 4 + 1);
        assertEquals(perRow * 100, DefaultPlanExecutor.estimateWorstCaseResultBytes(rowType, 100, 256));
    }

    /** Fixed-width widths: INT/FLOAT/DATE → 4+1, BIGINT/DOUBLE/TIMESTAMP → 8+1, BOOLEAN → 1+1. */
    public void testEstimateWorstCaseFixedWidths() {
        org.apache.calcite.rel.type.RelDataTypeFactory factory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT
        );
        org.apache.calcite.rel.type.RelDataType rowType = factory.builder()
            .add("i", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
            .add("d", org.apache.calcite.sql.type.SqlTypeName.DOUBLE)
            .add("b", org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
            .build();
        long perRow = (4 + 1) + (8 + 1) + (1 + 1);
        assertEquals(perRow * 10, DefaultPlanExecutor.estimateWorstCaseResultBytes(rowType, 10, 256));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private CircuitBreaker requestBreaker(long limitBytes) {
        HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes, ByteSizeUnit.BYTES)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return service.getBreaker(CircuitBreaker.REQUEST);
    }

    /** Two-column batch with vectors in physical order [age (BigInt), name (VarChar)]. */
    private VectorSchemaRoot makeAgeNameBatch(long age, String name) {
        Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(ageField, nameField));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        vsr.allocateNew();
        ((BigIntVector) vsr.getVector("age")).setSafe(0, age);
        ((VarCharVector) vsr.getVector("name")).setSafe(0, name.getBytes(StandardCharsets.UTF_8));
        vsr.setRowCount(1);
        return vsr;
    }

    private VectorSchemaRoot makeIntBatch(String fieldName, int... values) {
        return makeIntBatch(allocator, fieldName, values);
    }

    private VectorSchemaRoot makeIntBatch(BufferAllocator alloc, String fieldName, int... values) {
        Field field = new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, alloc);
        vsr.allocateNew();
        IntVector vec = (IntVector) vsr.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vec.setSafe(i, values[i]);
        }
        vsr.setRowCount(values.length);
        return vsr;
    }

    private static <T> List<T> toList(Iterable<T> it) {
        List<T> out = new ArrayList<>();
        Iterator<T> iter = it.iterator();
        while (iter.hasNext())
            out.add(iter.next());
        return out;
    }
}
