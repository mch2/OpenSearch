/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.dv;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Segment-parallel variant of the doc-values leaf (spec J1 "segment parallelism"): {@code N}
 * producer threads each scan a static partition of the shard's segments — one forward-only
 * {@link ColumnBatchSource} per thread, so doc-values iterators are never shared — and push
 * exported Arrow batches into a bounded queue. The native side still PULLS one batch per
 * {@link #next()}; producers block on the full queue, which is the JVM-side backpressure that
 * composes with the Flight-level backpressure downstream (slow consumer ⇒ suspended producers,
 * bounded memory). Batch order across segments is unspecified — fine for every shape this PoC
 * targets (documented in the spec).
 *
 * <p>Lifecycle: {@link #close()} (from the native {@code leaf_close}, fired on stream drop in every
 * path — success, error, cancel) interrupts producers, drains + releases queued exports, joins the
 * threads, then runs the caller's cleanup. Queued-but-unconsumed exports are freed via
 * {@code release()} (the C release callback) since Rust never took ownership of them.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParallelDocValuesFragmentExecutor implements AnalyticsSearchBackendPlugin.LeafCursor {

    private static final Logger LOGGER = LogManagerHolder.LOGGER;

    /** Holder defers Logger init cost off the query path when the class loads early. */
    private static final class LogManagerHolder {
        static final Logger LOGGER = LogManager.getLogger(ParallelDocValuesFragmentExecutor.class);
    }

    /** Poison pill marking one producer's completion. */
    private static final Object SENTINEL_HOLDER = new Object();

    /**
     * One decoded batch waiting in the queue: the exported array and — for dictionary-encoded
     * batches, whose physical schema differs from the advertised one — the per-batch schema export
     * the consumer imports with (mirrors the sequential {@link DocValuesFragmentExecutor}). Both C
     * structs are owned by whoever dequeues them and closed on the following pull / on close().
     */
    private static final class QueuedBatch {
        final ArrowArray array;
        final org.apache.arrow.c.ArrowSchema schema; // null in utf8 mode (advertised schema is authoritative)

        QueuedBatch(ArrowArray array, org.apache.arrow.c.ArrowSchema schema) {
            this.array = array;
            this.schema = schema;
        }
    }

    private final BufferAllocator allocator;
    private final Schema projectedSchema;
    /** Physical batch schema (== projected in utf8 mode; dictionary-encoded otherwise). */
    private final Schema physicalSchema;
    private final Runnable onClose;

    private final BlockingQueue<Object> queue;
    private final List<Thread> producers = new ArrayList<>();
    private final AtomicInteger liveProducers;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    /** The previous pull's export, released on the following next()/close() (deferred close). */
    private ArrowArray pendingExport;
    private org.apache.arrow.c.ArrowSchema pendingSchemaExport;
    private boolean exhausted;

    // Fragment counters (shared across producers).
    private final LongAdder docsRead = new LongAdder();
    private final LongAdder docsMatched = new LongAdder();
    private final LongAdder batchesEmitted = new LongAdder();
    private final LongAdder bytesEmitted = new LongAdder();
    private final List<ColumnBatchSource> sources = new ArrayList<>();

    /**
     * @param sourceFactory one fresh {@link ColumnBatchSource} per producer thread (doc-values
     *                      iterators are forward-only and never shared across threads)
     * @param parallelism   producer thread count, already clamped by the caller to
     *                      {@code min(segments, dv.segment_parallelism)}
     * @param onClose       releases the reader lease; runs exactly once, from {@link #close()}
     */
    public ParallelDocValuesFragmentExecutor(
        BufferAllocator allocator,
        IndexSearcher searcher,
        Query query,
        Schema projectedSchema,
        Supplier<ColumnBatchSource> sourceFactory,
        int batchSize,
        int parallelism,
        Runnable onClose
    ) throws IOException {
        this.allocator = allocator;
        this.projectedSchema = projectedSchema;
        this.onClose = onClose;
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        Query rewritten = searcher.rewrite(query);
        // Weight is thread-safe; scorers are created per segment inside each producer.
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        int threads = Math.max(1, Math.min(parallelism, leaves.size()));
        // 2 in-flight batches per producer: enough to overlap decode with the consumer's pull,
        // small enough that a stalled consumer suspends producers with bounded memory.
        this.queue = new ArrayBlockingQueue<>(Math.max(2, threads * 2));
        this.liveProducers = new AtomicInteger(threads);

        Schema physical = projectedSchema;
        for (int t = 0; t < threads; t++) {
            List<LeafReaderContext> mine = new ArrayList<>();
            for (int i = t; i < leaves.size(); i += threads) {
                mine.add(leaves.get(i)); // round-robin partition — balances segment sizes on average
            }
            ColumnBatchSource source = sourceFactory.get();
            physical = source.physicalSchema(projectedSchema); // identical across sources (same specs)
            synchronized (sources) {
                sources.add(source);
            }
            Thread producer = new Thread(
                () -> runProducer(mine, weight, source, batchSize),
                "dv-leaf-segment-scan-" + t + "-" + System.identityHashCode(this)
            );
            producer.setDaemon(true);
            producers.add(producer);
        }
        this.physicalSchema = physical;
        producers.forEach(Thread::start);
    }

    /** Producer body: scan assigned segments, decode, export, push; sentinel on exit. */
    private void runProducer(List<LeafReaderContext> mine, Weight weight, ColumnBatchSource source, int batchSize) {
        int[] docBuffer = new int[batchSize];
        try {
            for (LeafReaderContext leaf : mine) {
                if (closed) {
                    return;
                }
                docsRead.add(leaf.reader().maxDoc());
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                Bits liveDocs = leaf.reader().getLiveDocs();
                DocIdSetIterator it = scorer.iterator();
                int count = 0;
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (liveDocs != null && liveDocs.get(doc) == false) {
                        continue;
                    }
                    docBuffer[count++] = doc;
                    if (count == batchSize) {
                        emit(leaf, source, docBuffer, count);
                        count = 0;
                        if (closed) {
                            return;
                        }
                    }
                }
                if (count > 0) {
                    emit(leaf, source, docBuffer, count);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // close() interrupting a blocked put — clean exit
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        } finally {
            liveProducers.decrementAndGet();
            // Always deliver the sentinel so a parked consumer wakes even on failure/interrupt.
            // offer() suffices if the queue has room; otherwise spin-poll respecting close.
            while (closed == false && queue.offer(SENTINEL_HOLDER) == false) {
                try {
                    if (queue.offer(SENTINEL_HOLDER, 100, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Decode + export one batch and push it (blocking = backpressure). */
    private void emit(LeafReaderContext leaf, ColumnBatchSource source, int[] docBuffer, int count) throws IOException,
        InterruptedException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(physicalSchema, allocator)) {
            root.allocateNew();
            source.decodeBatch(leaf, docBuffer, count, root);
            root.setRowCount(count);
            docsMatched.add(count);
            batchesEmitted.increment();
            for (FieldVector v : root.getFieldVectors()) {
                bytesEmitted.add(v.getBufferSize());
            }
            // Dictionary mode: the physical schema differs from the advertised one, so export it per
            // batch (with THIS producer's per-batch dictionary provider) alongside the array — the
            // consumer imports with it and casts to the advertised schema (see JavaCursorStream).
            org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries = source.dictionaryProvider();
            ArrowArray array = ArrowArray.allocateNew(allocator);
            org.apache.arrow.c.ArrowSchema schemaStruct = null;
            boolean queued = false;
            try {
                if (physicalSchema != projectedSchema) {
                    schemaStruct = org.apache.arrow.c.ArrowSchema.allocateNew(allocator);
                    Data.exportSchema(allocator, physicalSchema, dictionaries, schemaStruct);
                }
                Data.exportVectorSchemaRoot(allocator, root, dictionaries, array);
                // Blocking put = producer suspension when the consumer is slow. On close(),
                // the interrupt unblocks us and the catch in runProducer exits cleanly.
                queue.put(new QueuedBatch(array, schemaStruct));
                queued = true;
            } finally {
                if (queued == false) {
                    array.release();
                    array.close();
                    if (schemaStruct != null) {
                        schemaStruct.release();
                        schemaStruct.close();
                    }
                }
            }
        }
    }

    @Override
    public long next() throws Exception {
        releasePending();
        if (closed || exhausted) {
            return 0L;
        }
        int sentinelsSeen = 0;
        while (true) {
            Object item = queue.poll(30, TimeUnit.SECONDS);
            if (item == null) {
                // No batch in 30s: either all producers already exited (their sentinels were
                // consumed in an earlier pull) or something is genuinely wedged.
                if (liveProducers.get() == 0 && queue.isEmpty()) {
                    return finish();
                }
                Throwable t = failure.get();
                if (t != null) {
                    exhausted = true;
                    throw asException(t);
                }
                continue; // producers alive and decoding a large batch — keep waiting
            }
            if (item == SENTINEL_HOLDER) {
                sentinelsSeen++;
                if (liveProducers.get() == 0 && queue.isEmpty()) {
                    return finish();
                }
                continue;
            }
            QueuedBatch batch = (QueuedBatch) item;
            pendingExport = batch.array;
            pendingSchemaExport = batch.schema;
            return batch.array.memoryAddress();
        }
    }

    private long finish() throws Exception {
        exhausted = true;
        Throwable t = failure.get();
        if (t != null) {
            throw asException(t);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "[dv-leaf parallel] scan complete: docsRead={} docsMatched={} batches={} bytes={}",
                docsRead.sum(),
                docsMatched.sum(),
                batchesEmitted.sum(),
                bytesEmitted.sum()
            );
        }
        return 0L;
    }

    @Override
    public long currentSchemaPtr() {
        // MUST delegate: a dictionary-mode batch's physical schema differs from the advertised one;
        // dropping this pointer makes the native importer read an Int32 dictionary array as Utf8View
        // (2 buffers vs variadic-view layout) and panic. Zero in utf8 mode (advertised is authoritative).
        return pendingSchemaExport == null ? 0L : pendingSchemaExport.memoryAddress();
    }

    private static Exception asException(Throwable t) {
        return t instanceof Exception e ? e : new RuntimeException(t);
    }

    private void releasePending() {
        if (pendingExport != null) {
            pendingExport.close();
            pendingExport = null;
        }
        if (pendingSchemaExport != null) {
            pendingSchemaExport.close();
            pendingSchemaExport = null;
        }
    }

    /**
     * Reclaim a queued item Rust never consumed: release() runs its still-armed C release callback
     * (freeing the buffers back to the allocator), then close() frees the wrapper. Sentinels hold no
     * memory. Applies to both the array and any per-batch dictionary schema struct.
     */
    private static void releaseQueued(Object item) {
        if (item == SENTINEL_HOLDER) {
            return;
        }
        QueuedBatch batch = (QueuedBatch) item;
        batch.array.release();
        batch.array.close();
        if (batch.schema != null) {
            batch.schema.release();
            batch.schema.close();
        }
    }

    // ── Counters ──

    public long docsRead() {
        return docsRead.sum();
    }

    public long docsMatched() {
        return docsMatched.sum();
    }

    public long batchesEmitted() {
        return batchesEmitted.sum();
    }

    public long bytesEmitted() {
        return bytesEmitted.sum();
    }

    /** Aggregated per-column decode stats across all producer sources. */
    public List<ColumnBatchSource.ColumnDecodeStats> decodeStats() {
        List<ColumnBatchSource.ColumnDecodeStats> merged = new ArrayList<>();
        synchronized (sources) {
            for (ColumnBatchSource source : sources) {
                List<ColumnBatchSource.ColumnDecodeStats> stats = source.decodeStats();
                if (merged.isEmpty()) {
                    merged.addAll(stats);
                    continue;
                }
                for (int i = 0; i < stats.size(); i++) {
                    ColumnBatchSource.ColumnDecodeStats a = merged.get(i);
                    ColumnBatchSource.ColumnDecodeStats b = stats.get(i);
                    merged.set(
                        i,
                        new ColumnBatchSource.ColumnDecodeStats(
                            a.column(),
                            a.bulkDecodeBatches() + b.bulkDecodeBatches(),
                            a.perDocFallbackBatches() + b.perDocFallbackBatches(),
                            a.decodeNanos() + b.decodeNanos()
                        )
                    );
                }
            }
        }
        return merged;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        releasePending();
        // Unblock producers parked in queue.put(), then reclaim every queued export: Rust never
        // consumed them, so their release callbacks are still armed — release() frees the buffers
        // back to the allocator (the leak class this project has scars from).
        for (Thread p : producers) {
            p.interrupt();
        }
        Object item;
        while ((item = queue.poll()) != null) {
            releaseQueued(item);
        }
        for (Thread p : producers) {
            try {
                p.join(TimeUnit.SECONDS.toMillis(10));
                if (p.isAlive()) {
                    LOGGER.warn("dv-leaf producer {} did not exit within 10s of close", p.getName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // A producer may have completed an export between our drain and its interrupt landing;
        // sweep once more after join so nothing armed remains queued.
        while ((item = queue.poll()) != null) {
            releaseQueued(item);
        }
        synchronized (sources) {
            for (ColumnBatchSource source : sources) {
                source.close();
            }
        }
        if (onClose != null) {
            onClose.run();
        }
    }
}
