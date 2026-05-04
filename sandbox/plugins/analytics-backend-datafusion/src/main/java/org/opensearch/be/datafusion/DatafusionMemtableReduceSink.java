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
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Memtable variant of {@link DatafusionReduceSink}: instead of opening streaming partitions
 * and pushing each shard response through them, this sink buffers every fed
 * {@link VectorSchemaRoot} per input as an exported Arrow C Data pair and on
 * {@link #close()} hands the full set across in one native call per input. The native side
 * builds a {@code MemTable} per input, registers each, and runs the Substrait plan against
 * the materialized inputs.
 *
 * <p>Trade-offs:
 * <ul>
 *   <li>+ No tokio mpsc, no cross-runtime spawn machinery in the input path. The single-shot
 *       handoff is simpler to reason about and matches the lifecycle already used for the
 *       output stream.</li>
 *   <li>− All input batches live in memory until {@code close()}. Use the streaming sink when
 *       the working set is too large to retain.</li>
 * </ul>
 *
 * <p>Lifecycle invariants and {@code feed}/{@code close} skeleton are implemented in
 * {@link AbstractDatafusionReduceSink}. This subclass owns the per-input buffered FFI structs
 * and the close-time {@code registerMemtable + executeLocalPlan + drain} sequence.
 */
public final class DatafusionMemtableReduceSink extends AbstractDatafusionReduceSink {

    /** Per-input buffered ArrowArray FFI structs, keyed by input index. */
    private final Map<Integer, List<ArrowArray>> arraysPerInput = new HashMap<>();
    /** Per-input buffered ArrowSchema FFI structs, keyed by input index. */
    private final Map<Integer, List<ArrowSchema>> schemasPerInput = new HashMap<>();

    public DatafusionMemtableReduceSink(ExchangeSinkContext ctx, NativeRuntimeHandle runtimeHandle) {
        super(ctx, runtimeHandle);
        for (int i = 0; i < ctx.inputs().size(); i++) {
            arraysPerInput.put(i, new ArrayList<>());
            schemasPerInput.put(i, new ArrayList<>());
        }
    }

    @Override
    protected void feedBatchUnderLock(int inputIndex, VectorSchemaRoot batch) {
        BufferAllocator alloc = ctx.allocator();
        ArrowArray array = ArrowArray.allocateNew(alloc);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(alloc);
        try {
            Data.exportVectorSchemaRoot(alloc, batch, null, array, arrowSchema);
            arraysPerInput.get(inputIndex).add(array);
            schemasPerInput.get(inputIndex).add(arrowSchema);
            array = null;
            arrowSchema = null;
        } finally {
            if (array != null) {
                array.close();
            }
            if (arrowSchema != null) {
                arrowSchema.close();
            }
        }
    }

    @Override
    protected Throwable closeUnderLock() {
        Throwable failure = null;
        long streamPtr = 0;
        try {
            // Register one memtable per input. Each memtable is keyed by its inputId
            // (e.g. "input-0", "input-1") which matches the substrait NamedScan reference
            // emitted by the fragment convertor.
            for (int i = 0; i < ctx.inputs().size(); i++) {
                List<ArrowArray> arrays = arraysPerInput.get(i);
                List<ArrowSchema> schemas = schemasPerInput.get(i);
                long[] arrayPtrs = new long[arrays.size()];
                long[] schemaPtrs = new long[schemas.size()];
                for (int j = 0; j < arrays.size(); j++) {
                    arrayPtrs[j] = arrays.get(j).memoryAddress();
                    schemaPtrs[j] = schemas.get(j).memoryAddress();
                }
                NativeBridge.registerMemtable(
                    session.getPointer(),
                    ctx.inputs().get(i).inputId(),
                    schemaIpcs[i],
                    arrayPtrs,
                    schemaPtrs
                );
            }

            streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), ctx.fragmentBytes());
            try (StreamHandle outStream = new StreamHandle(streamPtr, runtimeHandle)) {
                streamPtr = 0;
                drainOutputIntoDownstream(outStream);
            }
        } catch (Throwable t) {
            failure = accumulate(failure, t);
        } finally {
            // The Arrow Java wrappers must always be closed. On the success path Rust has
            // consumed the underlying FFI structs (release callback nulled), so close is a
            // no-op for the data. On the failure-before-handoff path close releases the
            // exported data buffers back to the Java allocator.
            for (List<ArrowArray> arrays : arraysPerInput.values()) {
                for (ArrowArray a : arrays) {
                    try {
                        a.close();
                    } catch (Throwable t) {
                        failure = accumulate(failure, t);
                    }
                }
                arrays.clear();
            }
            for (List<ArrowSchema> schemas : schemasPerInput.values()) {
                for (ArrowSchema s : schemas) {
                    try {
                        s.close();
                    } catch (Throwable t) {
                        failure = accumulate(failure, t);
                    }
                }
                schemas.clear();
            }
            if (streamPtr != 0) {
                NativeBridge.streamClose(streamPtr);
            }
        }
        return failure;
    }
}
