/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Static target of the Rust {@code df_register_leaf_bridge} UPCALLs (Model B, pull-based leaf).
 *
 * <p>The data-node Rust {@code Worker} terminates a distributed leaf task. To run the EXISTING,
 * unchanged {@code AnalyticsSearchService} reader-acquisition + delegation setup, the Rust
 * {@code ShardScanExec} makes ONE {@link #openFragment} upcall, then PULLS batches via
 * {@link #leafNext} (case 3) or adopts a native stream (cases 1 &amp; 2). The actual work is delegated
 * to an injected node-global {@link LeafBridge} implemented in the analytics-engine plugin (which owns
 * IndicesService + ReaderContextStore); this class is only the FFM-boundary shim (decode args, route,
 * never let an exception unwind across FFM).
 *
 * <p>Mirrors {@code FilterTreeCallbacks}: a single static node-global delegate installed once at
 * plugin start.
 */
public final class LeafBridgeCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(LeafBridgeCallbacks.class);

    /** Discriminators — MUST match the Rust {@code leaf_bridge::LEAF_MODE_*} constants. */
    public static final int LEAF_MODE_NATIVE = 1;
    public static final int LEAF_MODE_JAVA_CURSOR = 2;

    private static volatile LeafBridge BRIDGE;

    private LeafBridgeCallbacks() {}

    /**
     * Node-local leaf operations, implemented in analytics-engine and injected at startup. Keeps the
     * backend from compile-depending on the engine's data-node internals.
     */
    public interface LeafBridge {
        /** Result of opening a fragment: which mode + the handle (SessionContextHandle ptr for
         *  NATIVE, or an opaque Java cursor id for JAVA_CURSOR). */
        record Opened(int mode, long handle) {
        }

        /**
         * Run the unchanged AnalyticsSearchService setup for {@code (indexUuid, shardId)} under
         * {@code queryId} with the shard-local plan {@code substrait}, returning a discriminated
         * handle. NATIVE → DF executes from the returned SessionContextHandle; JAVA_CURSOR → Java
         * produces and the returned cursor is pulled via {@link #next}.
         *
         * <p>{@code descriptor} is the serialized {@code DelegationDescriptor} (empty = no delegation);
         * when present the leaf runs the INDEXED path — Java registers the {@code FilterDelegationHandle}
         * (keyed by {@code queryId}) and builds an indexed session with {@code treeShape}/{@code predicateCount}.
         */
        Opened open(long queryId, String indexUuid, int shardId, byte[] substrait, byte[] descriptor, int treeShape, int predicateCount)
            throws Exception;

        /** Pull one batch from a JAVA_CURSOR; returns the FFI_ArrowArray pointer, or 0 at EOS. */
        long next(long cursor) throws Exception;

        /** Release a JAVA_CURSOR's reader/context. */
        void close(long cursor);
    }

    public static void setBridge(LeafBridge bridge) {
        BRIDGE = bridge;
    }

    public static void clearBridge() {
        BRIDGE = null;
    }

    /**
     * FFM upcall (matches Rust {@code OpenFragmentFn = fn(i64, *const u8, i64, i32, *const u8, i64,
     * *mut i32, *mut i64) -> i32}). Writes mode + handle through the out-pointers; returns 0 / negative.
     */
    public static int openFragment(
        long queryId,
        MemorySegment indexUuidPtr,
        long indexUuidLen,
        int shardId,
        MemorySegment substraitPtr,
        long substraitLen,
        MemorySegment descriptorPtr,
        long descriptorLen,
        int treeShape,
        int predicateCount,
        MemorySegment outMode,
        MemorySegment outHandle
    ) {
        LeafBridge bridge = BRIDGE;
        if (bridge == null) {
            LOGGER.warn("openFragment upcall but no LeafBridge installed (queryId={}, shardId={})", queryId, shardId);
            return -1;
        }
        try {
            String indexUuid = indexUuidLen <= 0
                ? ""
                : new String(indexUuidPtr.reinterpret(indexUuidLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
            byte[] substrait = substraitLen <= 0 ? new byte[0] : substraitPtr.reinterpret(substraitLen).toArray(ValueLayout.JAVA_BYTE);
            byte[] descriptor = descriptorLen <= 0 ? new byte[0] : descriptorPtr.reinterpret(descriptorLen).toArray(ValueLayout.JAVA_BYTE);
            LeafBridge.Opened opened = bridge.open(queryId, indexUuid, shardId, substrait, descriptor, treeShape, predicateCount);
            // Raw `*mut` pointers arrive as zero-length segments across FFM; widen to the written
            // size before set() or the bounds check trips (byteSize 0, new length 4/8).
            outMode.reinterpret(Integer.BYTES).set(ValueLayout.JAVA_INT, 0, opened.mode());
            outHandle.reinterpret(Long.BYTES).set(ValueLayout.JAVA_LONG, 0, opened.handle());
            return 0;
        } catch (Throwable t) {
            LOGGER.error("openFragment failed for queryId=" + queryId + " shardId=" + shardId, t);
            return -2;
        }
    }

    /**
     * FFM upcall (matches Rust {@code LeafNextFn = fn(i64, *mut i64) -> i32}). Writes the
     * FFI_ArrowArray pointer (or 0 = EOS) through {@code outArray}; returns 0 / negative.
     */
    public static int leafNext(long cursor, MemorySegment outArray) {
        LeafBridge bridge = BRIDGE;
        if (bridge == null) {
            return -1;
        }
        try {
            long arrayPtr = bridge.next(cursor);
            outArray.reinterpret(Long.BYTES).set(ValueLayout.JAVA_LONG, 0, arrayPtr);
            return 0;
        } catch (Throwable t) {
            LOGGER.error("leafNext failed for cursor=" + cursor, t);
            return -2;
        }
    }

    /** FFM upcall (matches Rust {@code LeafCloseFn = fn(i64)}). */
    public static void leafClose(long cursor) {
        LeafBridge bridge = BRIDGE;
        if (bridge == null) {
            return;
        }
        try {
            bridge.close(cursor);
        } catch (Throwable t) {
            LOGGER.warn("leafClose failed for cursor=" + cursor, t);
        }
    }
}
