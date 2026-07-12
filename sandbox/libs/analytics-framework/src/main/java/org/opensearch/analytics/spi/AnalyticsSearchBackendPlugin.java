/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SPI extension point for backend query engine plugins.
 *
 * <p>A backend plugin is registered via {@link org.opensearch.plugins.ExtensiblePlugin} and
 * exposes three provider interfaces, each covering a distinct concern:
 * <ul>
 *   <li>{@link BackendCapabilityProvider} — capability declarations used by the coordinator-side
 *       planner to determine which backends can handle each operator, predicate, and expression.</li>
 *   <li>{@link SearchExecEngineProvider} — execution engine factory used at the data node to
 *       run a plan fragment and stream results.</li>
 *   <li>{@link FragmentConvertor} — plan serialization used by the planner and potentially
 *       at the data node to convert resolved RelNode fragments into backend-native form.</li>
 * </ul>
 *
 * <p>All three getter methods throw {@link UnsupportedOperationException} by default so that
 * backends fail fast if a component tries to use a capability the backend has not implemented.
 *
 * @opensearch.internal
 */
public interface AnalyticsSearchBackendPlugin {

    /** Unique backend name (e.g., "datafusion", "lucene"). */
    String name();

    /**
     * Returns the capability provider for this backend.
     * Used by the coordinator-side planner ({@code CapabilityRegistry}) to determine
     * which backends can handle each operator, predicate, aggregate call, and expression.
     */
    default BackendCapabilityProvider getCapabilityProvider() {
        throw new UnsupportedOperationException("getCapabilityProvider not implemented for [" + name() + "]");
    }

    /**
     * Returns the execution engine provider for this backend.
     * Used at the data node to create a {@link SearchExecEngineProvider} bound to an execution context.
     */
    default SearchExecEngineProvider getSearchExecEngineProvider() {
        throw new UnsupportedOperationException("getSearchExecEngineProvider not implemented for [" + name() + "]");
    }

    /**
     * Returns the fragment convertor for this backend.
     * Used by the planner to serialize resolved plan fragments into backend-native form,
     * and potentially at the data node for final executable conversion.
     */
    default FragmentConvertor getFragmentConvertor() {
        throw new UnsupportedOperationException("getFragmentConvertor not implemented for [" + name() + "]");
    }

    /**
     * Returns the exchange sink provider for this backend, or {@code null} if the backend
     * cannot act as a coordinator-side executor (i.e., cannot accept Arrow Record Batches
     * from data nodes and run computation over them).
     *
     * <p>Used by the planner to determine which backend handles the coordinator stage,
     * and by the Scheduler to create the sink when the query executes.
     */
    default ExchangeSinkProvider getExchangeSinkProvider() {
        return null;
    }

    /**
     * Returns the instruction handler factory for this backend. Used at the coordinator
     * to create instruction nodes (backend attaches custom config) and at the data node
     * to create handlers that apply instructions to the execution context.
     *
     * <p>Backends that declare {@code supportedDelegations} or participate in multi-stage
     * execution MUST implement this. Validation at startup ensures consistency.
     */
    default FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        throw new UnsupportedOperationException("getInstructionHandlerFactory not implemented for [" + name() + "]");
    }

    /**
     * Prepare a filter delegation handle for the given delegated expressions.
     * Called by Core after all instruction handlers have run, when the plan has delegation.
     *
     * <p>The accepting backend initializes its internal state (e.g., DirectoryReader,
     * QueryShardContext, compiled Queries) and returns a handle that the driving backend
     * will call into during execution.
     *
     * @param expressions the delegated expressions (annotationId + serialized query bytes)
     * @param ctx the shared execution context (Reader, MapperService, IndexSettings)
     * @return a handle the driving backend calls into via FFM upcalls
     */
    default FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        throw new UnsupportedOperationException("getFilterDelegationHandle not implemented for [" + name() + "]");
    }

    /**
     * Configure the driving backend to use the given delegation handle during execution.
     * Called by Core after obtaining the handle from the accepting backend.
     *
     * <p>The driving backend registers the handle so that FFM upcalls from Rust
     * (createProvider, createCollector, collectDocs) route to the correct per-query binding.
     *
     * @param contextId      the per-query identifier (task ID), threaded through every FFM upcall
     * @param handle         the delegation handle from the accepting backend
     * @param tracker        the thread tracker for resource attribution, or {@code null}
     * @param backendContext the driving backend's execution context (from instruction handlers)
     * @return a cleanup action that must be called (in a finally block) after query execution
     */
    default Runnable configureFilterDelegation(
        long contextId,
        FilterDelegationHandle handle,
        DelegationThreadTracker tracker,
        BackendExecutionContext backendContext
    ) {
        throw new UnsupportedOperationException("configureFilterDelegation not implemented for [" + name() + "]");
    }

    /**
     * Returns a snapshot of this backend's currently-tracked queries, keyed by {@code contextId}.
     *
     * <p>The map is a point-in-time view — entries can register or drain concurrently on the
     * backend side. Implementations MUST return a non-null map (empty when nothing is tracked)
     * and SHOULD make it unmodifiable so callers cannot mutate backend state.
     *
     * <p>Implementations MAY cap the result to a top-N subset by current memory usage to bound
     * the FFI cost (the DataFusion backend caps at the heaviest 10 live queries). Callers that
     * need a complete enumeration should not rely on this method.
     *
     * <p>Default implementation returns an empty map so backends that do not track per-query
     * metrics don't have to opt in.
     */
    default Map<Long, QueryExecutionMetrics> getTopQueriesByMemory() {
        return Collections.emptyMap();
    }

    /**
     * QTF fetch phase: reads specific rows by global row ID.
     * Row IDs are passed as a BigIntVector for zero-copy transfer to native.
     *
     * @param reader the index reader for the target shard
     * @param rowIdVector Arrow BigIntVector containing global row IDs
     * @param columns column names to read
     * @param allocator Arrow buffer allocator for result import
     * @return a result stream containing the requested rows
     */
    default EngineResultStream fetchByRowIds(
        Reader reader,
        BigIntVector rowIdVector,
        String[] columns,
        BufferAllocator allocator,
        long contextId
    ) {
        throw new UnsupportedOperationException("fetchByRowIds not implemented for [" + name() + "]");
    }

    /**
     * Coordinator-side distributed execution (the {@code datafusion-distributed} path). Given a
     * whole-query plan (phases marked but NOT pre-split into PARTIAL/FINAL — the engine decomposes
     * aggregates itself), the backend builds a distributed physical plan and executes its head stage,
     * returning a streaming result. The data plane is direct backend↔backend (e.g. rust↔rust gRPC);
     * the coordinator only drains the head-stage stream here.
     *
     * <p>The backend owns the native runtime + worker transport, so it (not the coordinator) holds the
     * pointers; the coordinator supplies the backend-agnostic inputs: the wire-format plan bytes from
     * {@link FragmentConvertor#convertFragment}, the ordered worker endpoint list, the shard→worker
     * routing map, and the query/context id.
     *
     * @param planBytes whole-query wire-format plan ({@link FragmentConvertor#convertFragment})
     * @param workerEndpoints ordered, distinct data-node worker endpoints (e.g. gRPC URLs)
     * @param shardRouting newline-joined {@code shardId:workerIdx} routing lines (worker-idx into
     *        {@code workerEndpoints}); shard order is task order
     * @param indexUuid single-table index uuid (diagnostics)
     * @param allocator Arrow buffer allocator for result import
     * @param contextId query id, for cancellation propagation
     * @return a streaming result for the head stage
     */
    default EngineResultStream distributedExecute(
        byte[] planBytes,
        List<String> workerEndpoints,
        String shardRouting,
        String indexUuid,
        BufferAllocator allocator,
        long contextId
    ) {
        return distributedExecute(planBytes, workerEndpoints, shardRouting, indexUuid, allocator, contextId, null, 0, 0, null);
    }

    /**
     * As {@link #distributedExecute(byte[], List, String, String, BufferAllocator, long)}, but carries
     * a predicate delegated to a secondary backend (the indexed-query path). The distributed planner
     * pushes the delegated predicate into the leaf scan; the leaf worker hands {@code delegationBytes}
     * back to the JVM to build the Lucene {@code FilterDelegationHandle} and runs an indexed scan on
     * the {@code leafFragmentBytes} shard-local plan.
     *
     * @param delegationBytes serialized {@link DelegationDescriptor} (null / empty = no delegation)
     * @param treeShape {@link FilterTreeShape} ordinal classifying the delegated filter for the leaf
     * @param predicateCount number of delegated predicates (leaf indexed classification)
     * @param leafFragmentBytes shard-local {@code Filter(markers)->scan} plan for the leaf indexed executor
     */
    default EngineResultStream distributedExecute(
        byte[] planBytes,
        List<String> workerEndpoints,
        String shardRouting,
        String indexUuid,
        BufferAllocator allocator,
        long contextId,
        byte[] delegationBytes,
        int treeShape,
        int predicateCount,
        byte[] leafFragmentBytes
    ) {
        return distributedExecute(
            planBytes,
            workerEndpoints,
            shardRouting,
            indexUuid,
            allocator,
            contextId,
            delegationBytes,
            treeShape,
            predicateCount,
            leafFragmentBytes,
            null,
            DistributedTuning.DEFAULT
        );
    }

    /**
     * Distributed-planner tuning knobs (see {@code datafusion-distributed}'s {@code DistributedConfig}).
     *
     * @param partialReduce insert an intermediate {@code PartialReduce} above each hash repartition,
     *        before the network shuffle, so high-cardinality group-bys merge partials locally and the
     *        shuffle carries far fewer rows (avoids a coordinator/shuffle bottleneck on wide keys).
     * @param cardinalityTaskCountFactor scale a stage's task count when a node changes cardinality
     *        (&gt;1 = fan wider for cardinality-increasing nodes); {@code 0} keeps the library default.
     * @param maxTasksPerStage hard cap on tasks per stage; {@code 0} inherits the worker count.
     * @param forcePartitionedJoins force PARTITIONED hash joins (zero the single-partition thresholds +
     *        enable repartition_joins) so a join over per-shard leaves shuffles both sides correctly;
     *        default true (required for correct join results on the distributed path).
     */
    record DistributedTuning(boolean partialReduce, double cardinalityTaskCountFactor, int maxTasksPerStage,
        boolean forcePartitionedJoins) {
        /** partial_reduce + force-partitioned-joins ON (the safe/high-cardinality defaults). */
        public static final DistributedTuning DEFAULT = new DistributedTuning(true, 0.0, 0, true);
    }

    /**
     * As above, plus the distributed-planner {@link DistributedTuning} knobs and the non-delegated
     * filter-pushdown leaf fragment.
     *
     * @param plainLeafFragmentBytes shard-local {@code Filter(real predicate)->Read} plan for a
     *        NON-delegated (datafusion) leaf, so the worker re-plans it against the ListingTable and
     *        DataFusion pushes the predicate into the parquet scan (row-group / page-index pruning).
     *        {@code null} / empty = no pushable WHERE filter. Mutually exclusive with delegation.
     */
    default EngineResultStream distributedExecute(
        byte[] planBytes,
        List<String> workerEndpoints,
        String shardRouting,
        String indexUuid,
        BufferAllocator allocator,
        long contextId,
        byte[] delegationBytes,
        int treeShape,
        int predicateCount,
        byte[] leafFragmentBytes,
        byte[] plainLeafFragmentBytes,
        DistributedTuning tuning
    ) {
        throw new UnsupportedOperationException("distributedExecute not implemented for [" + name() + "]");
    }

    /**
     * Cooperatively cancels in-flight backend work for {@code contextId} (e.g. fire the per-context
     * cancellation token). Called from a task cancellation listener for the fetch path, which —
     * unlike the query path's {@code SearchExecEngine} — returns an opaque {@link EngineResultStream}.
     * Implementations must signal the native execution to unwind, not close the stream cross-thread
     * (that races the in-flight pull). No-op for an unknown {@code contextId}; default no-op.
     */
    default void cancelByContext(long contextId) {}

    /**
     * Distributed-path leaf bridge (Model B). The data-node Rust {@code Worker} terminates a leaf task
     * and asks Java (via FFM upcall) to set up the scan, since reader acquisition + delegation must go
     * through the engine's unchanged data-node path. The engine provides the {@link LeafBridge}
     * implementation (it owns shard/reader access) and registers it with the backend here; the backend
     * forwards it to its native FFM upcall registry. Default no-op for backends without a distributed
     * engine.
     *
     * @param bridge engine-side leaf operations (open/next/close), or {@code null} to clear.
     */
    default void registerLeafBridge(LeafBridge bridge) {}

    /**
     * The bound gRPC port of this node's distributed Worker server, or {@code -1} if the backend has
     * no Worker running (distributed engine off / unsupported). The coordinator discovers a peer's
     * Worker URL by calling a transport action that returns this value, then dialing
     * {@code http://<node-host>:<port>}. Default {@code -1}.
     */
    default int getWorkerPort() {
        return -1;
    }

    /**
     * Builds a native session/scan handle from an already-acquired {@link Reader} (cases 1 &amp; 2:
     * DataFusion executes the parquet / indexed scan). The engine calls this from its
     * {@link LeafBridge#open} after acquiring the shard reader; the backend turns the reader into the
     * native handle the Rust leaf adopts (the existing {@code createSessionContext} path). Returns an
     * opaque native pointer (0 = unsupported).
     *
     * @param reader     the acquired shard reader (unchanged engine acquisition)
     * @param substrait  the shard-local plan (may be empty for a full scan)
     * @param indexed    true for the indexed/delegation path (case 2), false for plain parquet (case 1)
     * @param contextId  query/context id for tracking + cancellation
     */
    default long createNativeSessionHandle(
        org.opensearch.index.engine.exec.IndexReaderProvider.Reader reader,
        byte[] substrait,
        boolean indexed,
        long contextId
    ) {
        return createNativeSessionHandle(reader, substrait, indexed, contextId, 0, 0);
    }

    /**
     * As {@link #createNativeSessionHandle(org.opensearch.index.engine.exec.IndexReaderProvider.Reader, byte[], boolean, long)},
     * but for the indexed leaf carries the delegated-filter classification: {@code treeShape}
     * ({@code FilterTreeShape} ordinal) and {@code delegatedPredicateCount}. The {@code substrait} is the
     * shard-local {@code Filter(delegated_predicate markers) -> Read} fragment the indexed executor decodes.
     */
    default long createNativeSessionHandle(
        org.opensearch.index.engine.exec.IndexReaderProvider.Reader reader,
        byte[] substrait,
        boolean indexed,
        long contextId,
        int treeShape,
        int delegatedPredicateCount
    ) {
        throw new UnsupportedOperationException("createNativeSessionHandle not implemented for [" + name() + "]");
    }

    /**
     * Engine-side leaf operations injected into the backend via {@link #registerLeafBridge}. All types
     * are framework/core so neither plugin compile-depends on the other's internals. The backend's FFM
     * upcall shim ({@code LeafBridgeCallbacks}) routes Rust upcalls through this.
     */
    interface LeafBridge {
        /** Discriminated open result: {@code mode} (1=NATIVE, 2=JAVA_CURSOR) + the handle/cursor. */
        record Opened(int mode, long handle) {
        }

        /**
         * Run the unchanged data-node setup for {@code (indexUuid, shardId)} under {@code queryId} and
         * return a discriminated handle: NATIVE (DF executes, handle = native session ptr from
         * {@link #createNativeSessionHandle}) or JAVA_CURSOR (Java produces rows / Arrow doc-values,
         * handle = an opaque cursor pulled via {@link #next}).
         *
         * <p>{@code descriptor} is the serialized {@code DelegationDescriptor} (empty = no delegation);
         * when present the leaf runs the INDEXED path — register the Lucene {@code FilterDelegationHandle}
         * (keyed by {@code queryId}) and build an indexed session using {@code treeShape}/{@code predicateCount}.
         */
        Opened open(long queryId, String indexUuid, int shardId, byte[] substrait, byte[] descriptor, int treeShape, int predicateCount)
            throws Exception;

        /** Pull one batch from a JAVA_CURSOR; returns an Arrow C-Data {@code FFI_ArrowArray} pointer, or 0 at EOS. */
        long next(long cursor) throws Exception;

        /** Release a JAVA_CURSOR's reader/context. */
        void close(long cursor);
    }

    /**
     * Converts a backend-specific exception into an appropriate OpenSearch exception type.
     *
     * <p>Called by the engine when a fragment execution fails. If the backend recognizes
     * the error (e.g., memory limit exceeded, admission rejected), it returns a converted
     * exception with correct HTTP status semantics. Otherwise returns the original unchanged.
     *
     * <p>Default implementation performs no conversion.
     *
     * @param original the exception from fragment execution
     * @return converted exception, or {@code original} if no conversion applies
     */
    default Exception convertException(Exception original) {
        return original;
    }

    /**
     * Returns the backend's subtree convertor for combining multiple delegated predicates
     * into a single serialized expression, or {@code null} if the backend cannot combine.
     * When {@code null}, the framework falls back to one {@link DelegatedExpression} per leaf.
     */
    default DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return null;
    }

    /**
     * Per-function serializers for delegated predicates this backend can accept.
     * Keyed by {@link ScalarFunction} — the framework dispatches to the matching
     * serializer during fragment conversion when a predicate is delegated to this backend.
     */
    default Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return Map.of();
    }
}
