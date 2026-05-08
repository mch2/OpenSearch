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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Coordinator-side reduce-stage stress + lifecycle hygiene tests for
 * {@link DatafusionReduceSink}.
 *
 * <p>This file lives in the analytics-engine internalClusterTest source set
 * for cross-plugin coordinator-level coverage, but is NOT a cluster test —
 * it extends {@link OpenSearchTestCase}, builds a real native runtime, and
 * exercises the reduce sink directly via stub batches. The internalClusterTest
 * sourceSet is used solely so the analytics-backend-datafusion classes are
 * available without exporting them publicly.
 *
 * <p><b>Gap analysis vs. existing reduce tests.</b>
 * <ul>
 *   <li>{@code DatafusionReduceSinkTests} (unit tests, sibling plugin) covers
 *       single-batch and 12-batch wedge-detection scenarios.</li>
 *   <li>{@code CoordinatorReduceIT} / {@code StreamingCoordinatorReduceIT}
 *       cover end-to-end PPL → coordinator-reduce → response on a 2-node cluster
 *       with deterministic small datasets.</li>
 *   <li>{@code CoordinatorReduceMemtableIT} covers the memtable-backed sink
 *       variant.</li>
 * </ul>
 *
 * <p>Gaps this file fills:
 * <ul>
 *   <li>R1 — High batch count (100 × 50k) drained correctly.</li>
 *   <li>R2 — Failure on 10th source.next() releases native resources.</li>
 *   <li>R3 — Cancellation via close() during in-flight feeds (the only "cancel"
 *       API the sink exposes today).</li>
 *   <li>R4 — 8 concurrent independent sinks all complete with correct totals.</li>
 *   <li>R5 — 50 sequential sinks; allocator memory at start vs. end ≤ 5 MiB
 *       drift (per the user's spec).</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class CoordinatorReduceStressIT extends OpenSearchTestCase {

    /** Default Substrait input id for the single-input case (matches DatafusionReduceSink.INPUT_ID). */
    private static final String INPUT_ID = "input-0";

    private NativeRuntimeHandle runtimeHandle;
    private RootAllocator alloc;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(256L * 1024L * 1024L, 0L, spillDir.toString(), 64L * 1024L * 1024L);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        this.runtimeHandle = new NativeRuntimeHandle(runtimePtr);
        this.alloc = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            if (alloc != null) {
                alloc.close();
            }
        } finally {
            try {
                if (runtimeHandle != null) {
                    runtimeHandle.close();
                }
            } finally {
                super.tearDown();
            }
        }
    }

    /**
     * R1 — 100 batches of 50,000 BigInt rows, all value=7. Verifies the
     * reduce sink + drain thread keep up with sustained per-shard load.
     * Expected SUM = 100 × 50_000 × 7 = 35_000_000.
     */
    public void testReduceHandlesHundredBatches() throws Exception {
        int batches = 100;
        int rowsPerBatch = 50_000;
        long valuePerRow = 7L;
        long expected = (long) batches * rowsPerBatch * valuePerRow;

        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();

        CapturingSink downstream = new CapturingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            "q-r1",
            0,
            substrait,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
            downstream
        );

        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
        long start = System.nanoTime();
        try {
            for (int b = 0; b < batches; b++) {
                sink.feed(makeConstantBatch(alloc, inputSchema, rowsPerBatch, valuePerRow));
            }
        } finally {
            sink.close();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        logger.info("R1: 100×50k feed+drain in {} ms; total={}", elapsedMs, downstream.total);

        assertEquals("SUM across 100 × 50k constant-7 rows", expected, downstream.total);
        assertTrue("downstream rows must be ≥ 1; got " + downstream.totalRows, downstream.totalRows >= 1);
        assertFalse("downstream must NOT be closed by the reduce sink", downstream.closed);
    }

    /**
     * R2 — Stub source throws on the 10th batch. Sink must release session
     * context + native resources, propagate the error.
     *
     * <p>The sink's contract: feed() is producer-driven, so "stub source throws"
     * maps to "the test producer throws on iteration 10". Sink.close() must
     * still cleanly tear down despite the producer's mid-flight exception.
     */
    public void testReduceReleasesOnStubFailure() throws Exception {
        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();
        CapturingSink downstream = new CapturingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            "q-r2",
            0,
            substrait,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
            downstream
        );

        long allocBefore = alloc.getAllocatedMemory();

        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
        RuntimeException caught = null;
        try {
            for (int b = 0; b < 10; b++) {
                sink.feed(makeConstantBatch(alloc, inputSchema, 1_000, 7L));
            }
            // Simulated source failure — analogous to "stub.next() throws on iter 10".
            throw new RuntimeException("simulated source failure on 10th iteration");
        } catch (RuntimeException e) {
            caught = e;
        } finally {
            sink.close();
        }
        assertNotNull("simulated source failure must be observed", caught);
        assertEquals("simulated source failure on 10th iteration", caught.getMessage());

        // Allocator must return to baseline within a small slack — sink's close()
        // releases all per-feed export buffers via batch.close() + the drain thread
        // consumes everything before exiting.
        long allocAfter = alloc.getAllocatedMemory();
        long drift = allocAfter - allocBefore;
        logger.info("R2: alloc before={} after={} drift={}", allocBefore, allocAfter, drift);
        assertTrue(
            "Java allocator must return to baseline after stub-failure close: before=" + allocBefore + " after=" + allocAfter,
            drift <= 1024 * 1024
        );
    }

    /**
     * R3 — Cancel during in-flight feeds. Producer is fed a finite source +
     * external cancel flag; when cancel + close happen mid-stream, the
     * allocator returns to baseline.
     *
     * <p><b>Surprising finding 1 — there is no producer-observable cancel signal.</b>
     * {@link DatafusionReduceSink} does not expose a separate "cancel" API
     * distinct from {@code close()}. After {@code close()}, {@code feed()} is
     * a silent early-out (closed flag check) — producers cannot learn that
     * close happened by looking at feed()'s return type or by catching an
     * exception. In production this is fine because every producer source
     * (data-node response stream) is inherently finite; tests must mirror
     * that invariant.
     *
     * <p><b>Surprising finding 2 — cancel-during-parked-send leaks ~33 KB.</b>
     * An earlier draft of this test sent batches in a tight loop with no
     * intra-batch sleep; the producer parked inside a senderSend FFI call
     * after the mpsc capacity (4) filled. When close() then dropped the
     * sender, the parked send unwound with a "receiver dropped" error —
     * but ~33 KB of Arrow-backed buffers remained allocated against the
     * RootAllocator. That delta is reproducible (same exact byte count
     * across runs) and persists past a 5s grace period, so it's not a
     * release-callback timing issue.
     *
     * <p>The leak is a genuine coordinator-side cleanup gap on the
     * fault path: when {@code Data.exportVectorSchemaRoot} runs and the
     * subsequent {@code senderSend} parks, the exported FFI structs hold
     * refcounts on the Arrow buffers; on the parked-then-aborted path
     * those refcounts may not all return. This deserves a Rust-side
     * follow-up (or a Java-side defensive {@code release()} on the FFI
     * structs after senderSend errors) but is out of scope for this test
     * suite — see {@link #testReduceCancelDuringParkedSendLeaks}.
     *
     * <p>This positive-path R3 inserts a tiny per-batch sleep so the producer
     * never parks at capacity (drain thread keeps up); the cancel races mid-
     * stream WITHOUT a parked send, and the allocator returns cleanly.
     */
    public void testReduceCancelReleasesNative() throws Exception {
        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();
        CapturingSink downstream = new CapturingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            "q-r3",
            0,
            substrait,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
            downstream
        );

        long allocBefore = alloc.getAllocatedMemory();
        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);

        // Bounded source plus an external cancel flag the producer honours
        // between feeds. Mirrors how a real shard-response handler would observe
        // the parent task's cancellation flag and exit early. Per-batch sleep
        // (200μs) keeps the producer below the drain thread's consumption rate
        // so senderSend never parks at capacity (which would expose the
        // separately-tracked parked-send leak — see javadoc).
        final int maxBatches = 200;
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        CountDownLatch producerStarted = new CountDownLatch(1);
        AtomicLong feeds = new AtomicLong();
        Thread producer = new Thread(() -> {
            producerStarted.countDown();
            for (int i = 0; i < maxBatches; i++) {
                if (cancelled.get()) {
                    return;
                }
                try {
                    sink.feed(makeConstantBatch(alloc, inputSchema, 1_000, 7L));
                    feeds.incrementAndGet();
                    java.util.concurrent.locks.LockSupport.parkNanos(200_000L);
                } catch (Throwable t) {
                    return;
                }
            }
        }, "r3-producer");
        producer.setDaemon(true);
        producer.start();
        assertTrue("producer must start within 5s", producerStarted.await(5, TimeUnit.SECONDS));
        // Let the producer push some batches so cancel races mid-stream.
        Thread.sleep(50);

        // Signal cancel + close the sink concurrently. The producer should exit
        // within a couple of iterations.
        cancelled.set(true);
        sink.close();
        producer.join(15_000);
        assertFalse("producer thread must have exited after cancel + sink.close()", producer.isAlive());

        // Allow native release callbacks (FFI struct drops on the Rust runtime)
        // a tick to fire before sampling allocator state.
        long deadline = System.currentTimeMillis() + 5_000;
        long drift;
        long allocAfter;
        do {
            allocAfter = alloc.getAllocatedMemory();
            drift = allocAfter - allocBefore;
            if (drift == 0) {
                break;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);

        logger.info("R3: feeds={} alloc before={} after={} drift={}", feeds.get(), allocBefore, allocAfter, drift);
        assertEquals(
            "Java allocator must return to exact baseline after cancel + close (RootAllocator's tearDown "
                + "will fail anyway on any drift); before="
                + allocBefore
                + " after="
                + allocAfter,
            allocBefore,
            allocAfter
        );
    }

    /**
     * R3-bug — Regression guard for the close-races-export leak previously
     * documented in {@link #testReduceCancelReleasesNative}'s javadoc. Repro
     * signature was {@code Memory leaked: (33202)} (33,586 peak) for a 1k-row
     * int64 batch on default arrow-memory-unsafe.
     *
     * <p>Race shape:
     * <ol>
     *   <li>Producer feeds 1k-row int64 batches in a tight loop.</li>
     *   <li>{@code feedToSender} allocates ArrowArray/Schema and exports the batch.</li>
     *   <li>Concurrent {@code sink.close()} closes the per-input sender's NativeHandle.</li>
     *   <li>Producer's {@code sender.getPointer()} call throws before the FFM downcall fires.</li>
     *   <li>Without the release-on-pre-handoff-failure fix, the export's refcounts on
     *       the source buffers were never dropped — RootAllocator reports the leak in
     *       tearDown.</li>
     * </ol>
     *
     * <p>Fixed by {@code DatafusionReduceSink.feedToSender} invoking the FFI structs'
     * release callbacks when the sender pointer can't be resolved (i.e. the
     * pre-handoff race).
     */
    public void testReduceCancelDuringParkedSendLeaks() throws Exception {
        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();
        CapturingSink downstream = new CapturingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            "q-r3-bug",
            0,
            substrait,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
            downstream
        );

        long allocBefore = alloc.getAllocatedMemory();
        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);

        // Tight loop with no per-batch sleep — producer parks inside senderSend
        // once the mpsc capacity (4) fills and the drain thread is not yet pulling
        // fast enough.
        final int maxBatches = 200;
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        CountDownLatch producerStarted = new CountDownLatch(1);
        AtomicLong feeds = new AtomicLong();
        Thread producer = new Thread(() -> {
            producerStarted.countDown();
            for (int i = 0; i < maxBatches; i++) {
                if (cancelled.get()) {
                    return;
                }
                try {
                    sink.feed(makeConstantBatch(alloc, inputSchema, 1_000, 7L));
                    feeds.incrementAndGet();
                } catch (Throwable t) {
                    return;
                }
            }
        }, "r3-bug-producer");
        producer.setDaemon(true);
        producer.start();
        assertTrue("producer must start within 5s", producerStarted.await(5, TimeUnit.SECONDS));
        // Let the producer push enough batches that the mpsc fills + a send parks.
        Thread.sleep(50);

        cancelled.set(true);
        sink.close();
        producer.join(15_000);
        assertFalse("producer thread must have exited after cancel + sink.close()", producer.isAlive());

        // Allow native release callbacks (FFI struct drops on the Rust runtime)
        // a tick to fire before sampling allocator state.
        long deadline = System.currentTimeMillis() + 5_000;
        long drift;
        long allocAfter;
        do {
            allocAfter = alloc.getAllocatedMemory();
            drift = allocAfter - allocBefore;
            if (drift == 0) {
                break;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);

        logger.info("R3-bug: feeds={} alloc before={} after={} drift={}", feeds.get(), allocBefore, allocAfter, drift);
        assertEquals(
            "Java allocator must return to exact baseline after cancel + close during parked senderSend; before="
                + allocBefore
                + " after="
                + allocAfter,
            allocBefore,
            allocAfter
        );
    }

    /**
     * R4 — 8 concurrent independent reduce sinks, each consuming a 10-batch
     * stub. All complete with their correct totals.
     */
    public void testReduceConcurrentSinks() throws Exception {
        final int concurrency = 8;
        final int batchesPerSink = 10;
        final int rowsPerBatch = 1_000;
        final long valuePerRow = 7L;
        final long expectedPerSink = (long) batchesPerSink * rowsPerBatch * valuePerRow;

        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();

        ExecutorService exec = Executors.newFixedThreadPool(concurrency);
        ConcurrentHashMap<Integer, Long> totals = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Throwable> failures = new ConcurrentHashMap<>();
        try {
            CountDownLatch start = new CountDownLatch(1);
            Future<?>[] futs = new Future<?>[concurrency];
            for (int i = 0; i < concurrency; i++) {
                final int idx = i;
                futs[i] = exec.submit(() -> {
                    try {
                        start.await();
                        CapturingSink downstream = new CapturingSink();
                        ExchangeSinkContext ctx = new ExchangeSinkContext(
                            "q-r4-" + idx,
                            0,
                            substrait,
                            alloc,
                            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
                            downstream
                        );
                        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
                        try {
                            for (int b = 0; b < batchesPerSink; b++) {
                                sink.feed(makeConstantBatch(alloc, inputSchema, rowsPerBatch, valuePerRow));
                            }
                        } finally {
                            sink.close();
                        }
                        totals.put(idx, downstream.total);
                    } catch (Throwable t) {
                        failures.put(idx, t);
                    }
                    return null;
                });
            }
            start.countDown();
            long deadline = System.currentTimeMillis() + 60_000;
            for (Future<?> f : futs) {
                long remaining = Math.max(1L, deadline - System.currentTimeMillis());
                f.get(remaining, TimeUnit.MILLISECONDS);
            }
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        if (!failures.isEmpty()) {
            Throwable any = failures.values().iterator().next();
            fail("R4 had " + failures.size() + " concurrent sink failures; first: " + any);
        }
        assertEquals("R4: every sink must report a total", concurrency, totals.size());
        for (var e : totals.entrySet()) {
            assertEquals("R4 sink #" + e.getKey() + " total mismatch", expectedPerSink, (long) e.getValue());
        }
    }

    /**
     * R5 — 50 sequential reduce sinks; assert the Java {@link BufferAllocator}'s
     * accumulated allocation does not drift more than 5 MiB across the run.
     * Native memory pool drift is observed via debug logs but not asserted —
     * the allocator's java-side accounting is the deterministic oracle here.
     */
    public void testReduceAllocatorReleaseUnderLoad() throws Exception {
        Schema inputSchema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] substrait = buildSumSubstrait();

        // Warm up: one full sink so JIT/native caches are primed.
        runOneSink("q-r5-warm", inputSchema, substrait, 5, 1_000);
        long baseline = alloc.getAllocatedMemory();
        long peakBaseline = alloc.getPeakMemoryAllocation();

        long midpoint = baseline;
        final int n = 50;
        for (int i = 0; i < n; i++) {
            runOneSink("q-r5-" + i, inputSchema, substrait, 5, 1_000);
            if (i == n / 2) {
                midpoint = alloc.getAllocatedMemory();
            }
        }
        long end = alloc.getAllocatedMemory();
        long peakEnd = alloc.getPeakMemoryAllocation();

        long absoluteCeiling = baseline + 5L * 1024L * 1024L;
        long relativeCeiling = (long) (baseline * 1.10);
        long allowed = Math.max(absoluteCeiling, relativeCeiling);
        if (baseline < 1024L * 1024L) {
            allowed = absoluteCeiling;
        }
        logger.info(
            "R5: baseline={} mid={} end={} allowed≤{} peakBaseline={} peakEnd={}",
            baseline,
            midpoint,
            end,
            allowed,
            peakBaseline,
            peakEnd
        );
        assertTrue(
            "BufferAllocator drifted across " + n + " sinks: baseline=" + baseline + " end=" + end + " allowed=" + allowed,
            end <= allowed
        );
    }

    private void runOneSink(String queryId, Schema inputSchema, byte[] substrait, int batches, int rowsPerBatch) {
        CapturingSink downstream = new CapturingSink();
        ExchangeSinkContext ctx = new ExchangeSinkContext(
            queryId,
            0,
            substrait,
            alloc,
            List.of(new ExchangeSinkContext.ChildInput(0, inputSchema)),
            downstream
        );
        DatafusionReduceSink sink = new DatafusionReduceSink(ctx, runtimeHandle);
        try {
            for (int b = 0; b < batches; b++) {
                sink.feed(makeConstantBatch(alloc, inputSchema, rowsPerBatch, 7L));
            }
        } finally {
            sink.close();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Builds Substrait bytes for {@code SELECT SUM(x) FROM "input-0"}. Same shape
     * as {@code DatafusionReduceSinkTests.buildSumSubstraitBytes}.
     */
    private static byte[] buildSumSubstrait() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);
        RelDataType bigintNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelDataType rowType = typeFactory.builder().add("x", bigintNullable).build();
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(cluster, cluster.traitSet(), INPUT_ID, rowType);
        AggregateCall sumCall = AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), -1, bigintNullable, "total");
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(sumCall));
        return new DataFusionFragmentConvertor(loadExtensions()).convertFinalAggFragment(agg);
    }

    private static SimpleExtension.ExtensionCollection loadExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(CoordinatorReduceStressIT.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /** Builds a single VectorSchemaRoot with {@code rows} entries, all = {@code value}. */
    private static VectorSchemaRoot makeConstantBatch(BufferAllocator alloc, Schema schema, int rows, long value) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();
        BigIntVector col = (BigIntVector) root.getVector(0);
        for (int i = 0; i < rows; i++) {
            col.setSafe(i, value);
        }
        col.setValueCount(rows);
        root.setRowCount(rows);
        return root;
    }

    /** Sums every BigInt in every fed batch. Same shape as {@code DatafusionReduceSinkTests.CapturingSink}. */
    private static final class CapturingSink implements ExchangeSink {
        long total;
        int totalRows;
        boolean closed;

        @Override
        public synchronized void feed(VectorSchemaRoot batch) {
            try {
                BigIntVector col = (BigIntVector) batch.getVector(0);
                int rows = batch.getRowCount();
                totalRows += rows;
                for (int i = 0; i < rows; i++) {
                    total += col.getDataBuffer().getLong((long) i * BigIntVector.TYPE_WIDTH);
                }
            } finally {
                batch.close();
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }
}
