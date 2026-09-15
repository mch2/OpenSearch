/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.ArrowExport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Managed wrapper around an Apache Arrow {@link VectorSchemaRoot} with strict lifecycle enforcement.
 *
 * <p>Each instance follows the state machine: {@code ACTIVE → FROZEN → CLOSED}.
 * <ul>
 *   <li><strong>ACTIVE</strong> — Vectors are writable; row count can be incremented.</li>
 *   <li><strong>FROZEN</strong> — Read-only; data can be exported to the native writer via
 *       {@link #exportToArrow()} using the Arrow C Data Interface.</li>
 *   <li><strong>CLOSED</strong> — All Arrow resources (vectors and child allocator) are released.</li>
 * </ul>
 *
 * <p>State transitions are enforced: writing to a frozen VSR or closing an active VSR
 * (without freezing first) throws {@link IllegalStateException}.
 *
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class ManagedVSR implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(ManagedVSR.class);

    private final String id;
    private VectorSchemaRoot vsr;
    private final BufferAllocator allocator;
    private final AtomicReference<VSRState> state = new AtomicReference<>(VSRState.ACTIVE);
    private final Map<String, Leaf> fields = new HashMap<>();

    /**
     * A writable column, addressed by its full dotted name.
     *
     * <p>An {@code object} is stored as a struct, so {@code city.name} is a child vector rather than
     * a column of its own. Its enclosing structs carry their own validity: unless a struct is marked
     * defined for a row, that row's object is null and every child value under it is discarded on
     * export, however the child was written. So the enclosing structs travel with the vector.
     *
     * @param vector    the vector values are written into
     * @param enclosing the structs above it, outermost first; empty for a top-level column
     */
    private record Leaf(FieldVector vector, List<StructVector> enclosing) {
    }

    /**
     * Creates a new ManagedVSR.
     *
     * @param id unique identifier for this VSR
     * @param schema Arrow schema defining the vector structure
     * @param allocator buffer allocator for Arrow memory
     */
    public ManagedVSR(String id, Schema schema, BufferAllocator allocator) {
        this.id = id;
        this.vsr = VectorSchemaRoot.create(schema, allocator);
        this.allocator = allocator;
        indexAll();
    }

    /** Rebuilds the dotted-name index over the current vectors. */
    private void indexAll() {
        fields.clear();
        for (Field field : vsr.getSchema().getFields()) {
            index(field.getName(), vsr.getVector(field), List.of());
        }
    }

    /** Indexes {@code vector} under {@code path}, descending into a struct's children. */
    private void index(String path, FieldVector vector, List<StructVector> enclosing) {
        if (vector instanceof StructVector struct) {
            List<StructVector> childEnclosing = new ArrayList<>(enclosing);
            childEnclosing.add(struct);
            for (FieldVector child : struct.getChildrenFromFields()) {
                index(path + "." + child.getName(), child, childEnclosing);
            }
            return;
        }
        fields.put(path, new Leaf(vector, enclosing));
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return vsr.getRowCount();
    }

    /**
     * Sets the row count.
     *
     * @param rowCount the new row count
     */
    public void setRowCount(int rowCount) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        vsr.setRowCount(rowCount);
    }

    /**
     * Returns the vector for the given field name, or null if not found.
     *
     * <p>Accepts the full dotted name of a leaf inside an {@code object} ({@code city.name}) as well
     * as a top-level column, and returns the vector values are written into either way.
     *
     * @param fieldName the field name
     * @return the field vector, or null
     */
    public FieldVector getVector(String fieldName) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state.get());
        }
        Leaf leaf = fields.get(fieldName);
        return leaf == null ? null : leaf.vector();
    }

    /**
     * Marks the objects enclosing {@code fieldName} present at {@code rowIndex}.
     *
     * <p>Must be called for every value written into a struct child. An Arrow struct owns a validity
     * bit per row, and a row whose bit is clear is a null object: the export drops whatever its
     * children hold. Marking is idempotent, so several leaves of the same object each marking it is
     * fine, and a row nobody marks is left null — which is what an omitted object should read as.
     *
     * <p>No-op for a top-level column and for an unknown name; the caller has already rejected an
     * unknown field.
     */
    public void markObjectsPresent(String fieldName, int rowIndex) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        Leaf leaf = fields.get(fieldName);
        if (leaf == null) {
            return;
        }
        for (StructVector struct : leaf.enclosing()) {
            struct.setIndexDefined(rowIndex);
        }
    }

    /**
     * Clears the objects enclosing {@code fieldName} at {@code rowIndex}, so the row reads as having
     * no object.
     *
     * <p>The counterpart to {@link #markObjectsPresent} for scrubbing a partially written row that
     * was never counted: left set, the bit would make the next document reusing the row index look
     * like it carried an object of all-null leaves.
     */
    public void clearObjectsPresent(String fieldName, int rowIndex) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot modify VSR in state: " + state.get());
        }
        Leaf leaf = fields.get(fieldName);
        if (leaf == null) {
            return;
        }
        for (StructVector struct : leaf.enclosing()) {
            struct.setNull(rowIndex);
        }
    }

    /** Transitions this VSR from ACTIVE to FROZEN state. */
    public void moveToFrozen() {
        if (state.compareAndSet(VSRState.ACTIVE, VSRState.FROZEN) == false) {
            throw new IllegalStateException("Cannot freeze VSR " + id + ": expected ACTIVE but was " + state.get());
        }
        logger.debug("State transition: ACTIVE -> FROZEN for VSR {}", id);
    }

    /**
     * Exports this VSR to Arrow C Data Interface for native handoff.
     * Only allowed when VSR is FROZEN.
     */
    public ArrowExport exportToArrow() {
        if (state.get() != VSRState.FROZEN) {
            throw new IllegalStateException("Cannot export VSR in state: " + state.get() + ". Must be FROZEN.");
        }
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        return new ArrowExport(arrowArray, arrowSchema);
    }

    /**
     * Exports only the schema to Arrow C Data Interface.
     */
    public ArrowSchema exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, vsr.getSchema(), null, arrowSchema);
        return arrowSchema;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return the VSR state
     */
    public VSRState getState() {
        return state.get();
    }

    /**
     * Dynamically adds a new field to this VSR. Creates the vector using the internal
     * allocator and appends it to the schema. Only allowed in ACTIVE state.
     *
     * @param field the Arrow field descriptor
     */
    public void addFieldVector(Field field) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot add field to VSR in state: " + state.get());
        }
        FieldVector vector = field.createVector(allocator);
        List<FieldVector> vectors = new ArrayList<>(vsr.getFieldVectors());
        vectors.add(vector);
        List<Field> newFields = new ArrayList<>(vsr.getSchema().getFields());
        newFields.add(field);
        int rowCount = vsr.getRowCount();
        vsr = new VectorSchemaRoot(newFields, vectors, rowCount);
        index(field.getName(), vector, List.of());
    }

    /**
     * Adds a child vector to an {@code object} already in this VSR, for a sub-field dynamic mapping
     * introduced after the VSR was created. Only allowed in ACTIVE state.
     *
     * <p>The new child starts empty, so every row already written reads null for it — which is what
     * those documents meant, since the sub-field did not exist when they were indexed.
     *
     * <p>The root is rebuilt afterwards because {@link VectorSchemaRoot#getSchema()} is captured at
     * construction: leaving it stale would export a schema that disagrees with the vectors it
     * describes.
     *
     * @param objectPath dotted path of the enclosing object; must already be a struct in this VSR
     * @param child      the Arrow field describing the new sub-field
     * @throws IllegalArgumentException if {@code objectPath} is not a struct in this VSR
     */
    public void addStructChild(String objectPath, Field child) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot add field to VSR in state: " + state.get());
        }
        StructVector struct = findStruct(objectPath);
        if (struct == null) {
            throw new IllegalArgumentException("No struct vector at [" + objectPath + "] in VSR " + id);
        }
        if (struct.getChild(child.getName(), FieldVector.class) != null) {
            return;
        }
        // addOrGet is how Arrow grows a struct in place; FieldVector is enough for the cast it does.
        // It builds the vector from the FieldType alone, which carries no children, so a sub-object
        // has to have its own children attached — only ever on the newly created vector, since
        // initializing an existing one would discard the values already in it.
        FieldVector created = struct.addOrGet(child.getName(), child.getFieldType(), FieldVector.class);
        if (child.getChildren().isEmpty() == false) {
            created.initializeChildrenFromFields(child.getChildren());
        }
        rebuildRoot();
    }

    /**
     * Returns the struct vector holding the {@code object} at the given dotted path, or null when
     * there is none. Values are written to the object's leaves via {@link #getVector}; this exposes
     * the struct itself, whose validity says which rows carry the object.
     */
    public StructVector getStruct(String objectPath) {
        if (state.get() != VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot access vector in VSR state: " + state.get());
        }
        return findStruct(objectPath);
    }

    /** True when a struct vector exists at the given dotted object path. */
    public boolean hasStruct(String objectPath) {
        return getStruct(objectPath) != null;
    }

    /** Locates the struct vector at a dotted object path, or null when there is none. */
    private StructVector findStruct(String objectPath) {
        FieldVector current = null;
        for (String part : objectPath.split("\\.")) {
            if (current == null) {
                current = vsr.getVector(part);
            } else if (current instanceof StructVector struct) {
                current = struct.getChild(part, FieldVector.class);
            } else {
                return null;
            }
            if (current == null) {
                return null;
            }
        }
        return current instanceof StructVector struct ? struct : null;
    }

    /**
     * Rebuilds the root so its schema reflects the current vectors, then re-indexes. Vector identity
     * is preserved, so values already written survive.
     */
    private void rebuildRoot() {
        List<FieldVector> vectors = new ArrayList<>(vsr.getFieldVectors());
        List<Field> newFields = new ArrayList<>(vectors.size());
        for (FieldVector vector : vectors) {
            newFields.add(vector.getField());
        }
        vsr = new VectorSchemaRoot(newFields, vectors, vsr.getRowCount());
        indexAll();
    }

    /**
     * Returns the current Arrow schema of this VSR.
     *
     * @return the schema
     */
    public Schema getSchema() {
        return vsr.getSchema();
    }

    /**
     * Returns the unique identifier.
     *
     * @return the VSR id
     */
    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (state.get() == VSRState.CLOSED) {
            return;
        }
        if (state.get() == VSRState.ACTIVE) {
            throw new IllegalStateException("Cannot close VSR " + id + ": must freeze first");
        }
        if (state.compareAndSet(VSRState.FROZEN, VSRState.CLOSED) == false) {
            throw new IllegalStateException("Expected VSR to be FROZEN but was " + state.get());
        }
        logger.debug("State transition: FROZEN -> CLOSED for VSR {}", id);
        if (vsr != null) {
            vsr.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Override
    public String toString() {
        return "ManagedVSR{id='" + id + "', state=" + state.get() + ", rows=" + getRowCount() + "}";
    }
}
