/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Type-safe wrapper around a native {@code PartitionStreamSender} pointer. Closing
 * the sender signals EOF to the DataFusion receiver side.
 *
 * <p>Produced by {@link NativeBridge#registerPartitionStream(long, String, byte[])} and used
 * by {@link DatafusionReduceSink#feed} to push Arrow C Data batches into the reduce input
 * stream. Closing the sender signals EOF to the DataFusion receiver side.
 *
 * <h2>Concurrency contract</h2>
 *
 * <p>The native {@code sender_send} entry takes an immutable borrow of the heap-allocated
 * {@code PartitionStreamSender} ({@code &*(sender_ptr as *const _)}) and holds it across
 * the {@code tokio::sync::mpsc::Sender::send().await} inside {@code block_on}. Concurrently,
 * {@code sender_close} reclaims the {@code Box} via {@code Box::from_raw} and drops the
 * sender — which is a use-after-free if {@code sender_send} is mid-await.
 *
 * <p>This wrapper serialises the two operations via a read-write lock: {@link #send} takes
 * the read lock for the duration of the native call, so multiple producers can send in
 * parallel; {@link #close} takes the write lock, so it waits for every in-flight send to
 * return before reclaiming the native sender. Without this, a producer parked in
 * {@code send_blocking} while another thread closes the sender results in tokio mpsc
 * tearing down internal state with permits still held — observable as the assertion
 * {@code self.inner.semaphore.is_idle()} firing on a {@code datafusion-cpu} worker.
 */
public final class DatafusionPartitionSender extends NativeHandle {

    /**
     * Read-write lock that mediates {@link #send} / {@link #close} so the native pointer
     * cannot be freed while {@code sender_send} is mid-await. Producers contend on the
     * read lock (multiple concurrent sends are fine — the underlying tokio mpsc Sender
     * is itself thread-safe), close on the write lock.
     */
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

    /**
     * Wraps the given sender pointer.
     *
     * @param senderPtr pointer returned by {@link NativeBridge#registerPartitionStream}
     */
    public DatafusionPartitionSender(long senderPtr) {
        super(senderPtr);
    }

    /**
     * Sends a batch through the native sender. Acquires the read lock for the duration
     * of the FFM downcall so {@link #close} cannot reclaim the heap allocation while the
     * native side holds an immutable borrow across its {@code send().await}. Throws
     * {@link IllegalStateException} if the sender has already been closed.
     */
    public void send(long arrayAddr, long schemaAddr) {
        lifecycle.readLock().lock();
        try {
            assert lifecycle.getReadHoldCount() > 0 : "send must hold the read lock across the FFM downcall";
            NativeBridge.senderSend(getPointer(), arrayAddr, schemaAddr);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public void close() {
        // Hold the write lock across the entire close, including the native
        // sender_close downcall (which Box::from_raw's the heap allocation).
        // Read-lock holders (in-flight send()) will finish before we proceed,
        // so the FFM borrow is guaranteed to have ended before we drop.
        lifecycle.writeLock().lock();
        try {
            assert lifecycle.isWriteLockedByCurrentThread() : "close must hold the write lock across super.close()";
            super.close();
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    protected void doClose() {
        NativeBridge.senderClose(ptr);
    }
}
