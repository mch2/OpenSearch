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
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Streaming coordinator-side reduce sink: opens one native partition stream per input
 * up front, pushes each fed batch through the corresponding tokio mpsc-backed sender,
 * and on close drains the native output stream into {@link ExchangeSinkContext#downstream()}.
 *
 * <p>Overrides the base class's {@code synchronized(feedLock)} with a lock-free
 * implementation. Multiple shard response handlers call {@link #feed(int, VectorSchemaRoot)}
 * concurrently — possibly on different inputs simultaneously; backpressure comes from the
 * native Rust mpsc channel (bounded, capacity 4 per input). The send-after-close race is
 * handled by catching the native error when the receiver has been dropped.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Constructor registers one input partition stream per {@link ExchangeSinkContext#inputs()}
 *       entry and kicks off native execution. The substrait plan resolves
 *       {@code NamedScan("input-" + i)} against the registered streams by string equality.</li>
 *   <li>{@link #feed(int, VectorSchemaRoot)} exports each batch via Arrow C Data and sends
 *       it lock-free on {@code senders[inputIndex]}.</li>
 *   <li>{@link #close} closes every sender (EOF for that input), drains output, releases
 *       native resources.</li>
 * </ol>
 */
public final class DatafusionReduceSink extends AbstractDatafusionReduceSink {

    /** Substrait/DataFusion table name for the first registered input — preserves the
     *  legacy single-input convention used by tests that build a substrait plan referencing
     *  a single named scan. Multi-input substrait plans reference {@code "input-" + i}. */
    static final String INPUT_ID = "input-0";

    private static final Logger logger = LogManager.getLogger(DatafusionReduceSink.class);

    /** One sender per input, indexed parallel to {@link ExchangeSinkContext#inputs()}. */
    private final DatafusionPartitionSender[] senders;
    private final StreamHandle outStream;
    /** Cumulative batches fed across all inputs. */
    private final AtomicLong feedCount = new AtomicLong();
    /**
     * Count of feeds that have entered the lock-free critical section but have not yet
     * released their hold on a sender pointer. Incremented at the top of every feed,
     * decremented in the matching {@code finally}. {@link #closeUnderLock} spins until
     * this drops to zero before dropping the senders — without this barrier, close()
     * could free the Rust {@code mpsc::Sender} pointer while a concurrent feed still
     * has a non-null reference cached for {@code senderSend} (use-after-free in JNI).
     */
    private final AtomicInteger inFlightFeeds = new AtomicInteger();
    /**
     * Background thread that drains {@link #outStream} into the downstream sink as soon
     * as the FINAL plan emits batches — running concurrently with feeds.
     *
     * <p>Without this thread, the FINAL plan's downstream side is not polled until
     * {@code close()} runs {@link #drainOutputIntoDownstream}. That polling chain is
     * what causes DataFusion's input operators to pull from our partition streams'
     * receivers. Without a concurrent puller, producers wedge past the input mpsc
     * capacity (verified empirically with target_partitions=1; without RepartitionExec
     * or this drain thread, the 2nd send_blocking parks indefinitely).
     *
     * <p>The thread starts polling immediately at construction. It exits naturally
     * when the FINAL plan reaches EOF (after every {@link #senders sender}.close()
     * signals input EOF and DataFusion completes the last operator).
     */
    private final Thread drainThread;
    /** Captures any throwable from the drain thread for surfacing during close(). */
    private final AtomicReference<Throwable> drainFailure = new AtomicReference<>();

    public DatafusionReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        int n = ctx.inputs().size();
        long[] senderPtrs = new long[n];
        long streamPtr = 0;
        try {
            for (int i = 0; i < n; i++) {
                senderPtrs[i] = NativeBridge.registerPartitionStream(
                    session.getPointer(),
                    ctx.inputs().get(i).inputId(),
                    schemaIpcs[i]
                );
            }
            this.senders = new DatafusionPartitionSender[n];
            for (int i = 0; i < n; i++) {
                senders[i] = new DatafusionPartitionSender(senderPtrs[i]);
            }
            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            this.outStream = new StreamHandle(streamPtr, runtimeHandle);
        } catch (RuntimeException e) {
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
            for (long p : senderPtrs) {
                if (p != 0) {
                    try {
                        NativeBridge.senderClose(p);
                    } catch (Throwable ignore) {}
                }
            }
            session.close();
            throw e;
        }
        // Spawn the drain thread AFTER the native handles are constructed so the catch-block
        // doesn't have to deal with thread teardown on construction failure.
        this.drainThread = new Thread(this::drainLoop, "df-reduce-drain-q" + ctx.queryId() + "-s" + ctx.stageId());
        this.drainThread.setDaemon(true);
        this.drainThread.start();
    }

    /**
     * Drain loop body. Runs on {@link #drainThread} from sink construction until the
     * FINAL plan reaches EOF (which only happens after every sender's {@code close()} is
     * called by {@link #closeUnderLock}).
     *
     * <p>Polls {@link #outStream} via {@code streamNext} and forwards each emitted batch
     * to {@code ctx.downstream()}. Any throwable is captured in {@link #drainFailure}
     * and re-surfaced from {@link #closeUnderLock} via the existing accumulate pattern.
     */
    private void drainLoop() {
        try {
            drainOutputIntoDownstream(outStream);
        } catch (Throwable t) {
            drainFailure.set(t);
            logger.warn("[ReduceSink] drain thread terminated with error", t);
        }
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("DatafusionReduceSink is multi-input — use feed(int inputIndex, batch)");
    }

    /**
     * Lock-free indexed feed: overrides the base class's synchronized feed.
     * Arrow C Data export and native send happen without a Java mutex.
     * Backpressure comes from the Rust mpsc channel (per-input).
     *
     * <p>Use-after-free protection: {@link #inFlightFeeds} is incremented before any
     * sender access and decremented in the matching {@code finally}. {@link #closeUnderLock}
     * waits for the counter to reach zero before dropping the senders, so a feed in
     * progress here is guaranteed to see a live sender pointer through {@code senderSend}.
     * Combined with the {@code closed} fast-path check (read AFTER the increment), this
     * rules out the JMM race where a feed could observe {@code closed=false} and then
     * call {@code senderSend} on an already-freed pointer.
     */
    @Override
    public void feed(int inputIndex, VectorSchemaRoot batch) {
        // Increment FIRST so close() sees us before we read closed. Without this ordering
        // the volatile read of closed could observe pre-close state, then close() flips
        // closed=true and proceeds to drop senders before our incrementAndGet.
        inFlightFeeds.incrementAndGet();
        try {
            if (closed) {
                batch.close();
                return;
            }
            // Export Arrow C Data outside any lock. The allocator is thread-safe;
            // multiple shard handlers can export concurrently, possibly on different inputs.
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
            // No mutex — Tokio mpsc::Sender is Send + Sync; concurrent senderSend on
            // different sender pointers is safe. The senders[] array is guaranteed live
            // here because closeUnderLock has not yet dropped the senders (in-flight
            // counter is > 0 for the duration of this method).
            try {
                NativeBridge.senderSend(senders[inputIndex].getPointer(), array.memoryAddress(), arrowSchema.memoryAddress());
                feedCount.incrementAndGet();
            } catch (RuntimeException e) {
                // Rare: receiver-dropped errors during normal shutdown. The drain thread
                // has already finished consuming from the receiver if we reach here, so
                // the batch is harmlessly discarded.
                if (closed) {
                    logger.debug("[ReduceSink] send-after-close race caught, discarding batch");
                    return;
                }
                throw e;
            } finally {
                array.close();
                arrowSchema.close();
            }
        } finally {
            inFlightFeeds.decrementAndGet();
        }
    }

    /**
     * Not used — feed() is overridden directly. Required by the abstract class contract.
     */
    @Override
    protected void feedBatchUnderLock(int inputIndex, VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("DatafusionReduceSink overrides feed(int, batch) directly");
    }

    @Override
    protected Throwable closeUnderLock() {
        Throwable failure = null;
        // 0. Wait for any in-flight lock-free feeds to release their hold on a sender
        // pointer. The base class has already flipped `closed=true` under feedLock; new
        // feeds entering this critical section will short-circuit on the `closed` check
        // (after incrementing+decrementing the counter). The wait below is bounded by
        // however long an in-flight senderSend takes to complete the JNI call — typically
        // microseconds; spin-wait is appropriate since we're already on the close path.
        for (int spins = 0; inFlightFeeds.get() > 0; spins++) {
            if (spins < 64) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(1_000); // 1µs back-off after the spin budget
            }
        }
        // 1. Signal EOF on every input. The drain thread, which is already polling the
        // output stream, will receive the final batches and then EOF, then exit cleanly.
        for (DatafusionPartitionSender sender : senders) {
            try {
                sender.close();
            } catch (Throwable t) {
                failure = accumulate(failure, t);
            }
        }
        // 2. Wait for the drain thread to finish processing remaining output.
        try {
            drainThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = accumulate(failure, e);
        }
        // 3. Surface any error captured by the drain thread.
        Throwable drainErr = drainFailure.get();
        if (drainErr != null) {
            failure = accumulate(failure, drainErr);
        }
        // 4. Close native resources.
        try {
            outStream.close();
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        }
        return failure;
    }

    /** Returns the cumulative number of batches fed across all inputs. */
    public long feedCount() {
        return feedCount.get();
    }
}
