/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.be.datafusion.internal.LocalExecutionContext;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataFusion-backed local stage execution engine.
 * <p>
 * Runs a Substrait plan locally with streaming inputs fed from
 * child stages via FFM downcalls to the native DataFusion library.
 * <p>
 * Usage:
 * <ol>
 *   <li>Call {@link #registerInput} for each child stage before {@link #execute()}</li>
 *   <li>Call {@link #execute()} exactly once to start plan execution</li>
 *   <li>Call {@link #close()} to release native resources</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionLocalExecEngine implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(DatafusionLocalExecEngine.class);

    private final long sessionHandle;
    private final BufferAllocator allocator;
    private final byte[] substraitBytes;
    private final NativeRuntimeHandle runtimeHandle;
    private final ConcurrentHashMap<String, Long> senderHandlesByStageInputId = new ConcurrentHashMap<>();
    private volatile boolean executed = false;

    /**
     * Creates a local stage execution engine.
     *
     * @param ctx           the local execution context carrying query ID, fragment bytes, and allocator
     * @param runtimeHandle the native runtime handle for stream operations
     */
    public DatafusionLocalExecEngine(LocalExecutionContext ctx, NativeRuntimeHandle runtimeHandle) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(runtimeHandle, "runtimeHandle");
        this.allocator = ctx.getAllocator();
        this.substraitBytes = ctx.getFragmentBytes();
        this.runtimeHandle = runtimeHandle;
        this.sessionHandle = FfmBindings.createLocalSession();
        logger.info(
            "[DF.LocalExecEngine] CREATED queryId={} stageId={} sessionHandle={} substraitBytesLen={}",
            ctx.getQueryId(),
            ctx.getStageId(),
            sessionHandle,
            substraitBytes != null ? substraitBytes.length : 0
        );
    }

    public DatafusionInputHandle registerInput(String stageInputId, Schema schema) {
        if (executed) {
            throw new IllegalStateException("registerInput called after execute()");
        }
        byte[] schemaIpc = schemaToIpcBytes(schema);
        long senderHandle = FfmBindings.createPartitionStream(sessionHandle, stageInputId, schemaIpc);
        senderHandlesByStageInputId.put(stageInputId, senderHandle);
        logger.info(
            "[DF.LocalExecEngine] registerInput stageInputId={} schemaIpcLen={} senderHandle={}",
            stageInputId,
            schemaIpc.length,
            senderHandle
        );
        return new DatafusionInputHandle(senderHandle, allocator);
    }

    public EngineResultStream execute() {
        if (executed) {
            throw new IllegalStateException("execute() called twice");
        }
        executed = true;
        logger.info("[DF.LocalExecEngine] execute() ENTRY sessionHandle={}", sessionHandle);
        long outputHandle = FfmBindings.executeLocalPlan(sessionHandle, substraitBytes);
        logger.info("[DF.LocalExecEngine] execute() native returned outputHandle={}", outputHandle);
        StreamHandle streamHandle = new StreamHandle(outputHandle, runtimeHandle);
        return new DatafusionResultStream(streamHandle, allocator);
    }

    public void close() {
        logger.info("[DF.LocalExecEngine] close() sessionHandle={}", sessionHandle);
        FfmBindings.dropLocalSession(sessionHandle);
    }

    /**
     * Serialize an Arrow schema to IPC bytes for passing to the native side.
     */
    static byte[] schemaToIpcBytes(Schema schema) {
        try (BufferAllocator tempAllocator = allocator()) {
            org.apache.arrow.vector.VectorSchemaRoot root = org.apache.arrow.vector.VectorSchemaRoot.create(schema, tempAllocator);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                    writer.start();
                }
                return baos.toByteArray();
            } finally {
                root.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize Arrow schema to IPC bytes", e);
        }
    }

    private static BufferAllocator allocator() {
        return new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);
    }
}
