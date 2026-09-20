/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ManagedVSRTests extends OpenSearchTestCase {

    private RootAllocator rootAllocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        rootAllocator = new RootAllocator();
        schema = new Schema(List.of(new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    @Override
    public void tearDown() throws Exception {
        rootAllocator.close();
        super.tearDown();
    }

    public void testInitialState() {
        ManagedVSR vsr = createVSR("test-1");
        assertEquals(VSRState.ACTIVE, vsr.getState());
        assertEquals("test-1", vsr.getId());
        assertEquals(0, vsr.getRowCount());
        cleanup(vsr);
    }

    public void testSetRowCountInActive() {
        ManagedVSR vsr = createVSR("test-2");
        vsr.setRowCount(5);
        assertEquals(5, vsr.getRowCount());
        cleanup(vsr);
    }

    public void testGetVectorInActive() {
        ManagedVSR vsr = createVSR("test-3");
        FieldVector vec = vsr.getVector("val");
        assertNotNull(vec);
        assertTrue(vec instanceof IntVector);
        cleanup(vsr);
    }

    public void testGetVectorReturnsNullForUnknownField() {
        ManagedVSR vsr = createVSR("test-4");
        assertNull(vsr.getVector("nonexistent"));
        cleanup(vsr);
    }

    public void testMoveToFrozen() {
        ManagedVSR vsr = createVSR("test-5");
        vsr.moveToFrozen();
        assertEquals(VSRState.FROZEN, vsr.getState());
        vsr.close();
    }

    public void testSetRowCountThrowsInFrozen() {
        ManagedVSR vsr = createVSR("test-6");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, () -> vsr.setRowCount(10));
        vsr.close();
    }

    public void testGetVectorThrowsInFrozen() {
        ManagedVSR vsr = createVSR("test-7");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, () -> vsr.getVector("val"));
        vsr.close();
    }

    public void testMoveToFrozenThrowsIfAlreadyFrozen() {
        ManagedVSR vsr = createVSR("test-8");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, vsr::moveToFrozen);
        vsr.close();
    }

    public void testCloseFromFrozen() {
        ManagedVSR vsr = createVSR("test-9");
        vsr.moveToFrozen();
        vsr.close();
        assertEquals(VSRState.CLOSED, vsr.getState());
    }

    public void testCloseIsIdempotentWhenClosed() {
        ManagedVSR vsr = createVSR("test-10");
        vsr.moveToFrozen();
        vsr.close();
        vsr.close(); // should not throw
        assertEquals(VSRState.CLOSED, vsr.getState());
    }

    public void testCloseThrowsIfActive() {
        ManagedVSR vsr = createVSR("test-11");
        IllegalStateException e = expectThrows(IllegalStateException.class, vsr::close);
        assertTrue(e.getMessage().contains("must freeze first"));
        // cleanup: freeze then close
        vsr.moveToFrozen();
        vsr.close();
    }

    public void testToString() {
        ManagedVSR vsr = createVSR("test-12");
        String str = vsr.toString();
        assertTrue(str.contains("id='test-12'"));
        assertTrue(str.contains("state=ACTIVE"));
        assertTrue(str.contains("rows=0"));
        cleanup(vsr);
    }

    public void testWriteAndReadVector() {
        ManagedVSR vsr = createVSR("test-13");
        IntVector vec = (IntVector) vsr.getVector("val");
        vec.setSafe(0, 99);
        vsr.setRowCount(1);
        assertEquals(1, vsr.getRowCount());
        assertEquals(99, ((IntVector) vsr.getVector("val")).get(0));
        cleanup(vsr);
    }

    private ManagedVSR createVSR(String id) {
        BufferAllocator child = rootAllocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private void cleanup(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }

    public void testAddFieldVectorAddsNewField() {
        ManagedVSR vsr = createVSR("test-add-field");
        vsr.addFieldVector(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        assertNotNull(vsr.getVector("name"));
        assertEquals(2, vsr.getSchema().getFields().size());
        cleanup(vsr);
    }

    public void testAddFieldVectorPreservesExistingData() {
        ManagedVSR vsr = createVSR("test-preserve-data");
        IntVector vec = (IntVector) vsr.getVector("val");
        vec.setSafe(0, 100);
        vsr.setRowCount(1);
        vsr.addFieldVector(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        assertEquals(100, ((IntVector) vsr.getVector("val")).get(0));
        assertEquals(1, vsr.getRowCount());
        cleanup(vsr);
    }

    public void testAddFieldVectorThrowsOnFrozenState() {
        ManagedVSR vsr = createVSR("test-add-frozen");
        vsr.moveToFrozen();
        expectThrows(IllegalStateException.class, () -> vsr.addFieldVector(new Field("x", FieldType.nullable(new ArrowType.Utf8()), null)));
        vsr.close();
    }

    public void testGetSchemaReflectsDynamicAdditions() {
        ManagedVSR vsr = createVSR("test-schema-dynamic");
        vsr.addFieldVector(new Field("f1", FieldType.nullable(new ArrowType.Utf8()), null));
        vsr.addFieldVector(new Field("f2", FieldType.nullable(new ArrowType.Int(64, true)), null));
        assertEquals(3, vsr.getSchema().getFields().size());
        List<String> names = vsr.getSchema().getFields().stream().map(Field::getName).collect(java.util.stream.Collectors.toList());
        assertTrue(names.contains("val"));
        assertTrue(names.contains("f1"));
        assertTrue(names.contains("f2"));
        cleanup(vsr);
    }

    // ---- object fields, stored as Arrow structs ----

    /** A struct-shaped schema: an {@code object} named "city" over the given leaves. */
    private Schema objectSchema(List<Field> children) {
        return new Schema(
            List.of(
                new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("city", FieldType.nullable(ArrowType.Struct.INSTANCE), children)
            )
        );
    }

    private ManagedVSR createVSR(String id, Schema withSchema) {
        BufferAllocator child = rootAllocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, withSchema, child);
    }

    public void testObjectLeafIsAddressedByItsDottedName() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-leaf", withObject);

        // The whole point: callers keep writing to "city.name" without knowing it is a struct child.
        FieldVector leaf = vsr.getVector("city.name");
        assertNotNull(leaf);
        assertTrue(leaf instanceof VarCharVector);
        // The struct itself is not a writable column — values go to its leaves.
        assertNull(vsr.getVector("city"));
        cleanup(vsr);
    }

    public void testNestedObjectLeafIsAddressedByItsFullPath() {
        Field inner = new Field(
            "location",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            List.of(new Field("lat", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
        );
        ManagedVSR vsr = createVSR("test-struct-nested", objectSchema(List.of(inner)));
        assertNotNull(vsr.getVector("city.location.lat"));
        cleanup(vsr);
    }

    public void testMarkingAnObjectPresentMakesItReadable() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-present", withObject);

        ((VarCharVector) vsr.getVector("city.name")).setSafe(0, new Text("seattle"));
        vsr.markObjectsPresent("city.name", 0);
        vsr.setRowCount(1);

        StructVector city = vsr.getStruct("city");
        assertFalse("a row with a written leaf must have its object marked present", city.isNull(0));
        cleanup(vsr);
    }

    public void testAnUnwrittenObjectStaysNull() {
        // A document that omitted the object must read back as one null, not as an object whose
        // every leaf happens to be null: only the former answers isnull(city) true.
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-absent", withObject);

        ((IntVector) vsr.getVector("val")).setSafe(0, 7);
        vsr.setRowCount(1);

        StructVector city = vsr.getStruct("city");
        assertTrue(city.isNull(0));
        cleanup(vsr);
    }

    public void testClearingAnObjectUndoesThePresenceMark() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-scrub", withObject);

        ((VarCharVector) vsr.getVector("city.name")).setSafe(0, new Text("seattle"));
        vsr.markObjectsPresent("city.name", 0);
        vsr.clearObjectsPresent("city.name", 0);
        vsr.setRowCount(1);

        StructVector city = vsr.getStruct("city");
        assertTrue("a scrubbed row must not read as carrying an empty object", city.isNull(0));
        cleanup(vsr);
    }

    public void testAddingASubFieldToAnExistingObject() {
        // Dynamic mapping introducing city.zip mid-flight grows the struct rather than adding a
        // top-level column.
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-grow", withObject);

        ((VarCharVector) vsr.getVector("city.name")).setSafe(0, new Text("seattle"));
        vsr.markObjectsPresent("city.name", 0);
        vsr.setRowCount(1);

        vsr.addStructChild("city", new Field("zip", FieldType.nullable(new ArrowType.Utf8()), null));

        assertNotNull(vsr.getVector("city.zip"));
        assertEquals(
            "the value written before the sub-field existed survives",
            "seattle",
            ((VarCharVector) vsr.getVector("city.name")).getObject(0).toString()
        );
        // The exported schema has to agree with the vectors, or the C-interface handoff would
        // describe a struct with fewer children than it carries.
        Field city = vsr.getSchema().findField("city");
        assertEquals(2, city.getChildren().size());
        cleanup(vsr);
    }

    public void testAddingASubObjectBringsItsOwnChildren() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-grow-nested", withObject);

        vsr.addStructChild(
            "city",
            new Field(
                "location",
                FieldType.nullable(ArrowType.Struct.INSTANCE),
                List.of(new Field("lat", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null))
            )
        );

        assertNotNull("a new sub-object must arrive with its leaves", vsr.getVector("city.location.lat"));
        assertTrue(vsr.hasStruct("city.location"));
        cleanup(vsr);
    }

    public void testAddingASubFieldTwiceIsANoOp() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-idempotent", withObject);

        ((VarCharVector) vsr.getVector("city.name")).setSafe(0, new Text("seattle"));
        vsr.setRowCount(1);
        vsr.addStructChild("city", new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));

        assertEquals("re-adding must not discard values", "seattle", ((VarCharVector) vsr.getVector("city.name")).getObject(0).toString());
        cleanup(vsr);
    }

    public void testAddingASubFieldToSomethingThatIsNotAnObjectFails() {
        Schema withObject = objectSchema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        ManagedVSR vsr = createVSR("test-struct-not-object", withObject);
        expectThrows(
            IllegalArgumentException.class,
            () -> vsr.addStructChild("val", new Field("x", FieldType.nullable(new ArrowType.Utf8()), null))
        );
        cleanup(vsr);
    }
}
