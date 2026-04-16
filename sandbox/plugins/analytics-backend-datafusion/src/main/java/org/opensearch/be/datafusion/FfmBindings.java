/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * FFM-based bindings to the DataFusion native library's local stage
 * execution exports. No JNI — all calls use {@link java.lang.foreign.Linker}
 * downcall handles.
 * <p>
 * Loaded once at class init. All {@link MethodHandle} fields are {@code static final}.
 * <p>
 * Caller must ensure the module has {@code --enable-native-access} granted.
 * In tests this is {@code ALL-UNNAMED}; in production it is the backend module name.
 */
final class FfmBindings {

    static final Linker LINKER = Linker.nativeLinker();
    static final SymbolLookup LOOKUP;

    static {
        // Library name: libanalytics_df.so / analytics_df.dll / libanalytics_df.dylib
        // Arena.global() keeps the library loaded for process lifetime.
        // The native library is the same cdylib produced by the Rust build
        // (libopensearch_datafusion_jni) — it exports both JNI and C ABI symbols.
        LOOKUP = loadLibrary();
    }

    private static SymbolLookup loadLibrary() {
        try {
            return SymbolLookup.libraryLookup("opensearch_datafusion_jni", Arena.global());
        } catch (IllegalArgumentException e) {
            // Library not found by platform name — try loading via System.loadLibrary
            // which searches java.library.path (already set by the plugin's build.gradle).
            // After System.loadLibrary succeeds, the symbols are in the default lookup.
            try {
                System.loadLibrary("opensearch_datafusion_jni");
                return SymbolLookup.loaderLookup();
            } catch (UnsatisfiedLinkError ule) {
                throw new ExceptionInInitializerError(
                    "Failed to load native library for FFM bindings: "
                        + e.getMessage()
                        + ". Also failed via System.loadLibrary: "
                        + ule.getMessage()
                );
            }
        }
    }

    private static MethodHandle downcall(String name, FunctionDescriptor desc) {
        MemorySegment sym = LOOKUP.find(name).orElseThrow(() -> new UnsatisfiedLinkError("FFM symbol not found: " + name));
        return LINKER.downcallHandle(sym, desc);
    }

    // ---- MethodHandle fields for each exported Rust function ----

    private static final MethodHandle CREATE_LOCAL_SESSION = downcall("analytics_create_local_session", FunctionDescriptor.of(JAVA_LONG));

    private static final MethodHandle CREATE_PARTITION_STREAM = downcall(
        "analytics_create_partition_stream",
        FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT)
    );

    private static final MethodHandle PUSH_BATCH = downcall(
        "analytics_push_batch",
        FunctionDescriptor.ofVoid(JAVA_LONG, JAVA_LONG, JAVA_LONG)
    );

    private static final MethodHandle CLOSE_PARTITION_STREAM = downcall(
        "analytics_close_partition_stream",
        FunctionDescriptor.ofVoid(JAVA_LONG)
    );

    private static final MethodHandle EXECUTE_LOCAL_PLAN = downcall(
        "analytics_execute_local_plan",
        FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_INT)
    );

    private static final MethodHandle DROP_LOCAL_SESSION = downcall("analytics_drop_local_session", FunctionDescriptor.ofVoid(JAVA_LONG));

    // ---- Thin static wrapper methods ----

    /**
     * Create a local session context on the Rust side.
     *
     * @return a positive session handle
     * @throws RuntimeException if the native call fails (returns 0)
     */
    static long createLocalSession() {
        try {
            long h = (long) CREATE_LOCAL_SESSION.invokeExact();
            if (h <= 0) {
                throw new RuntimeException("analytics_create_local_session returned non-positive handle: " + h);
            }
            return h;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Create an FfiPartitionStream under a session.
     *
     * @param session      the session handle from {@link #createLocalSession()}
     * @param stageInputId the stable stage input ID (e.g. {@code __stage_0_input__})
     * @param schemaIpc    Arrow schema serialized as IPC bytes
     * @return a positive sender handle
     * @throws RuntimeException if the native call fails
     */
    static long createPartitionStream(long session, String stageInputId, byte[] schemaIpc) {
        try (Arena arena = Arena.ofConfined()) {
            byte[] idBytes = stageInputId.getBytes(StandardCharsets.UTF_8);
            MemorySegment idSeg = arena.allocate(idBytes.length);
            MemorySegment.copy(idBytes, 0, idSeg, JAVA_BYTE, 0, idBytes.length);

            MemorySegment schemaSeg = arena.allocate(schemaIpc.length);
            MemorySegment.copy(schemaIpc, 0, schemaSeg, JAVA_BYTE, 0, schemaIpc.length);

            long h = (long) CREATE_PARTITION_STREAM.invokeExact(session, idSeg, idBytes.length, schemaSeg, schemaIpc.length);
            if (h <= 0) {
                throw new RuntimeException("analytics_create_partition_stream failed for stageInputId=" + stageInputId);
            }
            return h;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Push a record batch via Arrow C Data Interface pointers.
     *
     * @param senderHandle the sender handle from {@link #createPartitionStream}
     * @param arrayPtr     raw address of the ArrowArray (C Data Interface)
     * @param schemaPtr    raw address of the ArrowSchema (C Data Interface)
     */
    static void pushBatch(long senderHandle, long arrayPtr, long schemaPtr) {
        try {
            PUSH_BATCH.invokeExact(senderHandle, arrayPtr, schemaPtr);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Close a partition stream's sender (EOF). Idempotent.
     *
     * @param senderHandle the sender handle
     */
    static void closePartitionStream(long senderHandle) {
        try {
            CLOSE_PARTITION_STREAM.invokeExact(senderHandle);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Execute the Substrait plan under the session.
     *
     * @param session        the session handle
     * @param substraitBytes serialized Substrait plan
     * @return a positive output stream handle
     * @throws RuntimeException if the native call fails
     */
    static long executeLocalPlan(long session, byte[] substraitBytes) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(substraitBytes.length);
            MemorySegment.copy(substraitBytes, 0, seg, JAVA_BYTE, 0, substraitBytes.length);

            long h = (long) EXECUTE_LOCAL_PLAN.invokeExact(session, seg, substraitBytes.length);
            if (h <= 0) {
                throw new RuntimeException("analytics_execute_local_plan failed");
            }
            return h;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Drop a session and all its resources. Idempotent.
     *
     * @param session the session handle
     */
    static void dropLocalSession(long session) {
        try {
            DROP_LOCAL_SESSION.invokeExact(session);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    // ---- Helpers ----

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error e) {
            throw e;
        }
        return new RuntimeException(t);
    }

    private FfmBindings() {}
}
