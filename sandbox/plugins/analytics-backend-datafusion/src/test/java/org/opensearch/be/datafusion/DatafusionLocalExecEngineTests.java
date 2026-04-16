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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.internal.InputHandle;
import org.opensearch.be.datafusion.internal.LocalExecutionContext;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

/**
 * Tests for {@link DatafusionLocalExecEngine}.
 * <p>
 * These tests require the native library to be available (built from the Rust side).
 */
public class DatafusionLocalExecEngineTests extends OpenSearchTestCase {

    private static boolean runtimeInitialized = false;
    private NativeRuntimeHandle runtimeHandle;
    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
        Path spillDir = createTempDir("datafusion-spill");
        long ptr = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024);
        runtimeHandle = new NativeRuntimeHandle(ptr);
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        runtimeHandle.close();
        super.tearDown();
    }

    /**
     * 28.1: Register two inputs before execute — returns two distinct handles.
     */
    public void testRegisterInputBeforeExecute() {
        LocalExecutionContext ctx = new LocalExecutionContext("q1", 1, new byte[0], allocator);
        DatafusionLocalExecEngine engine = new DatafusionLocalExecEngine(ctx, runtimeHandle);
        try {
            Schema schema = testSchema();
            InputHandle handle1 = engine.registerInput("__stage_0_input__", schema);
            InputHandle handle2 = engine.registerInput("__stage_1_input__", schema);
            assertNotNull("Handle 1 must not be null", handle1);
            assertNotNull("Handle 2 must not be null", handle2);
            assertNotSame("Handles must be distinct objects", handle1, handle2);
            // Clean up handles
            handle1.closeInput();
            handle2.closeInput();
        } finally {
            engine.close();
        }
    }

    /**
     * 28.2: Register input after execute throws IllegalStateException.
     */
    public void testRegisterInputAfterExecuteThrows() {
        // We need a valid substrait plan for execute() to succeed.
        // Since we can't easily create one here, we test the state check
        // by using a dummy plan that will fail — but registerInput should
        // still throw before we get to execute.
        // Actually, let's test the state flag directly:
        LocalExecutionContext ctx = new LocalExecutionContext("q1", 1, new byte[] { 0 }, allocator);
        DatafusionLocalExecEngine engine = new DatafusionLocalExecEngine(ctx, runtimeHandle);
        try {
            Schema schema = testSchema();
            engine.registerInput("__stage_0_input__", schema);
            // Force the executed flag by calling execute — it will fail because
            // the substrait bytes are garbage, but the flag should still be set.
            try {
                engine.execute();
            } catch (RuntimeException expected) {
                // Expected — bad substrait bytes
            }
            // Now registerInput should throw
            expectThrows(IllegalStateException.class, () -> engine.registerInput("__stage_1_input__", schema));
        } finally {
            engine.close();
        }
    }

    /**
     * 28.4: close() calls dropLocalSession without throwing.
     */
    public void testCloseDropsSession() {
        LocalExecutionContext ctx = new LocalExecutionContext("q1", 1, new byte[0], allocator);
        DatafusionLocalExecEngine engine = new DatafusionLocalExecEngine(ctx, runtimeHandle);
        // close should not throw
        engine.close();
        // Double close should also not throw (idempotent on the Rust side)
        engine.close();
    }

    // ---- Helpers ----

    private static Schema testSchema() {
        return new Schema(List.of(new Field("a", new FieldType(true, new ArrowType.Int(64, true), null), null)));
    }
}
