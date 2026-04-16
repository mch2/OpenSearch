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
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * Tests for {@link FfmBindings} — the FFM-based bindings to the DataFusion
 * native library's local stage execution exports.
 * <p>
 * These tests require the native library to be available (built from the Rust side).
 * They exercise the real FFM downcall path against the actual native library.
 */
public class FfmBindingsTests extends OpenSearchTestCase {

    private static boolean runtimeInitialized = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
    }

    /**
     * 26.1: Round-trip create and drop a local session.
     * Verify positive handle, no exception on drop.
     */
    public void testCreateAndDropLocalSession() {
        long session = FfmBindings.createLocalSession();
        assertTrue("Session handle must be positive", session > 0);
        // Drop should not throw
        FfmBindings.dropLocalSession(session);
    }

    /**
     * 26.2: Create a partition stream and verify it returns a positive sender handle.
     * Uses a real Arrow schema serialized to IPC bytes.
     */
    public void testCreatePartitionStreamReturnsPositiveHandle() throws Exception {
        long session = FfmBindings.createLocalSession();
        try {
            byte[] schemaIpc = schemaToIpcBytes(testSchema());
            long senderHandle = FfmBindings.createPartitionStream(session, "__stage_0_input__", schemaIpc);
            assertTrue("Sender handle must be positive", senderHandle > 0);
            // Clean up
            FfmBindings.closePartitionStream(senderHandle);
        } finally {
            FfmBindings.dropLocalSession(session);
        }
    }

    /**
     * 26.3: Verify that closePartitionStream is idempotent — calling it twice
     * does not throw.
     */
    public void testClosePartitionStreamIsIdempotent() throws Exception {
        long session = FfmBindings.createLocalSession();
        try {
            byte[] schemaIpc = schemaToIpcBytes(testSchema());
            long senderHandle = FfmBindings.createPartitionStream(session, "__stage_0_input__", schemaIpc);
            FfmBindings.closePartitionStream(senderHandle);
            // Second close should be a no-op
            FfmBindings.closePartitionStream(senderHandle);
        } finally {
            FfmBindings.dropLocalSession(session);
        }
    }

    /**
     * 26.4: Verify that dropLocalSession is idempotent — calling it twice
     * does not throw.
     */
    public void testDropLocalSessionIsIdempotent() {
        long session = FfmBindings.createLocalSession();
        FfmBindings.dropLocalSession(session);
        // Second drop should be a no-op
        FfmBindings.dropLocalSession(session);
    }

    /**
     * Verify that multiple partition streams can be created under the same session,
     * each returning a distinct positive handle.
     */
    public void testMultiplePartitionStreamsReturnDistinctHandles() throws Exception {
        long session = FfmBindings.createLocalSession();
        try {
            byte[] schemaIpc = schemaToIpcBytes(testSchema());
            long handle1 = FfmBindings.createPartitionStream(session, "__stage_0_input__", schemaIpc);
            long handle2 = FfmBindings.createPartitionStream(session, "__stage_1_input__", schemaIpc);
            assertTrue("Handle 1 must be positive", handle1 > 0);
            assertTrue("Handle 2 must be positive", handle2 > 0);
            assertNotEquals("Handles must be distinct", handle1, handle2);
            FfmBindings.closePartitionStream(handle1);
            FfmBindings.closePartitionStream(handle2);
        } finally {
            FfmBindings.dropLocalSession(session);
        }
    }

    /**
     * Verify that multiple sessions can coexist with distinct handles.
     */
    public void testMultipleSessionsReturnDistinctHandles() {
        long session1 = FfmBindings.createLocalSession();
        long session2 = FfmBindings.createLocalSession();
        try {
            assertTrue("Session 1 must be positive", session1 > 0);
            assertTrue("Session 2 must be positive", session2 > 0);
            assertNotEquals("Session handles must be distinct", session1, session2);
        } finally {
            FfmBindings.dropLocalSession(session1);
            FfmBindings.dropLocalSession(session2);
        }
    }

    // ---- Helpers ----

    private static Schema testSchema() {
        return new Schema(List.of(new Field("a", new FieldType(true, new ArrowType.Int(64, true), null), null)));
    }

    private static byte[] schemaToIpcBytes(Schema schema) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            org.apache.arrow.vector.VectorSchemaRoot root = org.apache.arrow.vector.VectorSchemaRoot.create(schema, allocator);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                    writer.start();
                    // Don't write any batches — we only need the schema
                }
                return baos.toByteArray();
            } finally {
                root.close();
            }
        }
    }
}
