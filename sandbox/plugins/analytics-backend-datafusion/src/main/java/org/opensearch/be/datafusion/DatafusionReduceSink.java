/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.MultiInputExchangeSink;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.arrow.c.Data.importField;

/**
 * Streaming coordinator-side reduce sink: opens one native partition stream per child
 * input, pushes each fed batch through a tokio mpsc-backed sender, and on close drains
 * the native output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Single-input shapes register one partition under {@link AbstractDatafusionReduceSink#INPUT_ID} and accept
 * batches via the inherited {@link #feed(VectorSchemaRoot)} method. Multi-input shapes
 * (Union) register one partition per child stage and require callers to obtain a
 * per-child wrapper via {@link #sinkForChild(int)} — feeds via the bare
 * {@link #feed(VectorSchemaRoot)} method are rejected since the routing target is
 * ambiguous.
 *
 * <p>Overrides the base class's {@code synchronized(feedLock)} with a lock-free
 * implementation for the per-sender feed path. Multiple shard response handlers call
 * {@link #feed} concurrently; backpressure comes from the native Rust mpsc channel
 * (bounded, capacity 4). The send-after-close race is handled by catching the native
 * error when the receiver has been dropped.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor registers all input partition streams and kicks off native execution.</li>
 *   <li>{@link #feed} (or {@link ChildSink#feed} via {@link #sinkForChild}) exports each
 *       batch via Arrow C Data and sends it lock-free to the appropriate sender.</li>
 *   <li>{@link #close} signals EOF on every still-open sender, drains output, and releases
 *       native resources.</li>
 * </ol>
 */
public final class DatafusionReduceSink extends AbstractDatafusionReduceSink implements MultiInputExchangeSink {

    private static final Logger logger = LogManager.getLogger(DatafusionReduceSink.class);

    /**
     * Per-child senders keyed by childStageId, populated in declaration order so the
     * single-input case can pick the sole entry without an explicit lookup.
     */
    private final Map<Integer, DatafusionPartitionSender> sendersByChildStageId;
    private final StreamHandle outStream;
    /** Cumulative batches fed into any native sender. */
    private final AtomicLong feedCount = new AtomicLong();
    /**
     * Output schema, cached on first drain. Acquired once via {@code streamGetSchema}
     * and reused for every imported batch — schema is fixed for the lifetime of the
     * stream, so a single fetch is sufficient.
     */
    private volatile Schema outSchema;
    /**
     * Single-flight gate for the opportunistic drain in {@link #drainAvailable}.
     * Producer threads CAS-acquire it before pulling from the output stream;
     * concurrent producers that don't acquire skip and rely on the holder to
     * finish draining. The Rust {@code stream_try_next} contract requires
     * single-threaded access on a given stream.
     */
    private final AtomicBoolean draining = new AtomicBoolean(false);
    /**
     * Set when {@link NativeBridge#streamTryNext} returns {@code 0} — drain has
     * seen end-of-stream and subsequent calls should skip the native poll.
     */
    private final AtomicBoolean eofSeen = new AtomicBoolean(false);

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(childInputs.size());
        long streamPtr = 0;
        try {
            // Register one native partition per child stage. The Substrait plan in
            // ctx.fragmentBytes() references each partition by its "input-<stageId>" name
            // (DataFusionFragmentConvertor names them this way during plan conversion).
            for (Map.Entry<Integer, byte[]> child : childInputs.entrySet()) {
                int childStageId = child.getKey();
                byte[] schemaIpc = child.getValue();
                long senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), inputIdFor(childStageId), schemaIpc);
                senders.put(childStageId, new DatafusionPartitionSender(senderPtr));
            }
            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            this.outStream = new StreamHandle(streamPtr, runtimeHandle);
        } catch (RuntimeException e) {
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
            for (DatafusionPartitionSender sender : senders.values()) {
                try {
                    sender.close();
                } catch (Throwable ignore) {}
            }
            session.close();
            throw e;
        }
        this.sendersByChildStageId = senders;
    }

    /**
     * Acquires (lazily, on first call) the output stream's schema so subsequent
     * {@link #drainAvailable} invocations can import each batch into a fresh
     * {@link VectorSchemaRoot}. Cached once — schema is fixed for the lifetime of
     * the stream.
     */
    private Schema acquireOutSchemaIfNeeded() {
        Schema cached = outSchema;
        if (cached != null) return cached;
        synchronized (this) {
            if (outSchema != null) return outSchema;
            BufferAllocator alloc = ctx.allocator();
            try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(outStream.getPointer(), listener));
                try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr)) {
                    Field structField = importField(alloc, arrowSchema, dictProvider);
                    outSchema = new Schema(structField.getChildren(), structField.getMetadata());
                }
            }
            return outSchema;
        }
    }

    /**
     * Opportunistically pulls every batch that's ready RIGHT NOW from the native
     * output stream and forwards each to {@link ExchangeSinkContext#downstream()}.
     * Returns immediately when the stream has nothing ready (Pending), reaches EOF,
     * or another producer thread is already draining.
     *
     * <p>Callers (the per-sender feed path) invoke this after pushing into the
     * input mpsc — DataFusion's pull-based execution model means the plan only
     * processes input when its output is actively polled, so producers must
     * collectively keep the output drained to avoid deadlocking on bounded mpsc
     * backpressure.
     *
     * <p>Concurrent producers single-flight via {@link #draining}: only one
     * thread polls the stream at a time (the {@code stream_try_next} contract
     * requires that), but every producer's call ensures at least one drain pass
     * happens after its push.
     */
    private void drainAvailable() {
        if (closed || eofSeen.get()) return;
        if (!draining.compareAndSet(false, true)) return;
        try {
            Schema schema = acquireOutSchemaIfNeeded();
            BufferAllocator alloc = ctx.allocator();
            while (!closed) {
                long arrayAddr = NativeBridge.streamTryNext(outStream.getPointer());
                if (arrayAddr == NativeBridge.STREAM_PENDING) return;
                if (arrayAddr == 0) {
                    eofSeen.set(true);
                    return;
                }
                try (
                    CDataDictionaryProvider dictProvider = new CDataDictionaryProvider();
                    ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)
                ) {
                    VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, alloc);
                    Data.importIntoVectorSchemaRoot(alloc, arrowArray, vsr, dictProvider);
                    ctx.downstream().feed(vsr);
                }
            }
        } finally {
            draining.set(false);
        }
    }

    /**
     * Lock-free feed for the single-input case: writes to the sole registered sender.
     * Multi-input callers must use {@link #sinkForChild(int)} instead — calling this
     * method when more than one partition is registered is a programming error because
     * the routing target is ambiguous.
     */
    @Override
    public void feed(VectorSchemaRoot batch) {
        if (sendersByChildStageId.size() != 1) {
            batch.close();
            throw new IllegalStateException(
                "DatafusionReduceSink has " + sendersByChildStageId.size() + " input partitions; use sinkForChild(int) instead of feed()"
            );
        }
        feedToSender(sendersByChildStageId.values().iterator().next(), batch);
    }

    @Override
    public ExchangeSink sinkForChild(int childStageId) {
        DatafusionPartitionSender sender = sendersByChildStageId.get(childStageId);
        if (sender == null) {
            throw new IllegalArgumentException(
                "No registered partition for childStageId=" + childStageId + "; known ids=" + sendersByChildStageId.keySet()
            );
        }
        return new ChildSink(sender);
    }

    /**
     * Lock-free per-sender feed. Exports the batch via Arrow C Data outside any lock
     * (the allocator is thread-safe; multiple shard handlers can export concurrently),
     * then sends it through the supplied sender. The Rust mpsc::Sender is thread-safe,
     * so multiple producers feeding the same sender is safe. If close() raced and
     * already ran senderClose, the native side returns an error ("receiver dropped")
     * which we catch and discard.
     */
    private void feedToSender(DatafusionPartitionSender sender, VectorSchemaRoot batch) {
        // Best-effort fast path — skip export work if already closed.
        if (closed) {
            batch.close();
            return;
        }
        BufferAllocator alloc = ctx.allocator();
        ArrowArray array = ArrowArray.allocateNew(alloc);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
        try {
            Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
        } catch (Throwable t) {
            array.close();
            arrowSchema.close();
            batch.close();
            throw t;
        } finally {
            batch.close();
        }
        // After this point ownership of the underlying buffers transfers to the FFI
        // structs' release callbacks. On the success path (and on any path where the
        // senderSend FFM downcall was actually invoked) Rust takes ownership via
        // FFI_ArrowArray::from_raw and is responsible for invoking release when the
        // imported batch is dropped — including on the send-after-close error path.
        //
        // The race we must defend against here is the *pre-handoff* one: between
        // exporting the batch and calling senderSend, sink.close() can run
        // concurrently and close `sender` (the per-input NativeHandle). Resolving
        // sender.getPointer() then throws IllegalStateException before the FFM
        // downcall, leaving the release callback never invoked and the source
        // buffers' refcounts leaked. Snapshot the pointer up front so we can
        // distinguish "Rust got the pointers (their problem)" from "Rust never
        // saw the pointers (our problem to release)".
        long senderPtr;
        try {
            senderPtr = sender.getPointer();
        } catch (RuntimeException e) {
            // Sender closed concurrently — release the exported buffers ourselves.
            releaseExportedFfiStructs(array, arrowSchema);
            array.close();
            arrowSchema.close();
            if (closed) {
                logger.debug("[ReduceSink] send-after-close race caught (sender closed), discarding batch");
                return;
            }
            throw e;
        }
        try {
            NativeBridge.senderSend(senderPtr, array.memoryAddress(), arrowSchema.memoryAddress());
            feedCount.incrementAndGet();
        } catch (RuntimeException e) {
            if (closed) {
                logger.debug("[ReduceSink] send-after-close race caught, discarding batch");
                return;
            }
            throw e;
        } finally {
            // After senderSend returned (Ok or Err), Rust has invoked from_raw on the
            // FFI structs and is responsible for the release callback — Java only
            // frees the 80/72-byte struct memory.
            array.close();
            arrowSchema.close();
        }
        // Opportunistic drain on the producer's own thread: pulls any output batches
        // that DataFusion's plan has emitted in response to recently-pushed input.
        // Replaces the dedicated drain thread — DataFusion's pull-based execution
        // model means the plan only processes input mpsc when its output stream is
        // actively polled, so producers must keep the output drained to avoid
        // deadlocking on bounded mpsc backpressure (cap=4 per partition_stream.rs).
        drainAvailable();
    }

    /**
     * Invokes the C release callback installed by {@code Data.exportVectorSchemaRoot}
     * so the export's refcounts on the source Arrow buffers are dropped. Used only
     * when Rust never received the pointers (e.g. the sender was concurrently
     * closed) — once the FFM downcall has fired, release is Rust's responsibility.
     */
    private static void releaseExportedFfiStructs(ArrowArray array, ArrowSchema arrowSchema) {
        try {
            array.release();
        } catch (Throwable t) {
            logger.warn("[ReduceSink] error releasing exported ArrowArray on failure path", t);
        }
        try {
            arrowSchema.release();
        } catch (Throwable t) {
            logger.warn("[ReduceSink] error releasing exported ArrowSchema on failure path", t);
        }
    }

    /**
     * Per-child wrapper returned from {@link #sinkForChild(int)}. The orchestrator
     * routes one of these per child stage, and the wrapper's close() signals EOF for
     * its specific input partition. Idempotent — duplicate close() calls are no-ops.
     */
    private final class ChildSink implements ExchangeSink {
        private final DatafusionPartitionSender sender;
        private volatile boolean childClosed;

        ChildSink(DatafusionPartitionSender sender) {
            this.sender = sender;
        }

        @Override
        public void feed(VectorSchemaRoot batch) {
            feedToSender(sender, batch);
        }

        @Override
        public void close() {
            if (childClosed) {
                return;
            }
            childClosed = true;
            try {
                sender.close();
            } catch (Throwable t) {
                logger.warn("[ReduceSink] error closing child sender", t);
            }
        }
    }

    /**
     * Not used — feed() is overridden directly for the single-input path and
     * {@link ChildSink#feed} for the multi-input path. Required by the abstract
     * class contract.
     */
    @Override
    protected void feedBatchUnderLock(VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("DatafusionReduceSink overrides feed() directly");
    }

    @Override
    protected Throwable closeUnderLock() {
        Throwable failure = null;
        logger.info("[ReduceSink] closeUnderLock START senders={} eofSeen={}", sendersByChildStageId.size(), eofSeen.get());
        // 1. Signal EOF on every still-open sender. Senders that were already
        // closed by their ChildSink wrapper are no-ops (idempotent on the Rust side).
        for (Map.Entry<Integer, DatafusionPartitionSender> entry : sendersByChildStageId.entrySet()) {
            try {
                logger.info("[ReduceSink] closing sender for child stage {}", entry.getKey());
                entry.getValue().close();
                logger.info("[ReduceSink] closed sender for child stage {}", entry.getKey());
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        }
        // 2. Final blocking drain — until the FINAL plan reaches EOF.
        if (!eofSeen.get()) {
            try {
                logger.info("[ReduceSink] drainOutputIntoDownstream START");
                drainOutputIntoDownstream(outStream);
                logger.info("[ReduceSink] drainOutputIntoDownstream END");
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        } else {
            logger.info("[ReduceSink] drainOutputIntoDownstream skipped (eofSeen)");
        }
        // 3. Close native resources.
        try {
            logger.info("[ReduceSink] outStream.close START");
            outStream.close();
            logger.info("[ReduceSink] outStream.close END");
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
        logger.info("[ReduceSink] closeUnderLock END");
        return failure;
    }

    /** Returns the cumulative number of batches fed into any native sender. */
    public long feedCount() {
        return feedCount.get();
    }
}
