/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.arrow.flight.transport.ArrowAllocatorProvider;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Per-query context. Created once in {@link DefaultPlanExecutor#execute}
 * and threaded through execution components. Holds immutable config (DAG,
 * executor, parent task) and the lazy per-query {@link BufferAllocator}.
 *
 * <p>The execution registry has moved to {@link PlanWalker}, which owns
 * the per-query execution map internally. This context is now purely
 * configuration plus one lazy allocator.
 *
 * @opensearch.internal
 */
public class QueryContext {

    private static final Logger logger = LogManager.getLogger(QueryContext.class);

    /**
     * Process-wide latch flipped by {@link #closeBufferAllocator} whenever Arrow's
     * allocator close throws "Memory was leaked". Intended for test-only oracles —
     * {@code closeBufferAllocator} fires inside the query-failure listener chain,
     * and its thrown exception is attached as a Suppressed that test clients can
     * swallow. This flag survives the physical close (Arrow releases tracked memory
     * before it throws), so {@code @After} hooks can read it without racing teardown.
     * Reset via {@link #clearLeakSignal} at test setup.
     */
    private static final java.util.concurrent.atomic.AtomicReference<String> LEAK_SIGNAL =
        new java.util.concurrent.atomic.AtomicReference<>();

    /** True if any query-context's allocator close in this JVM threw a leak. Test-only. */
    public static String detectedLeak() {
        return LEAK_SIGNAL.get();
    }

    /** Reset the leak latch. Call from test setup so each test starts clean. */
    public static void clearLeakSignal() {
        LEAK_SIGNAL.set(null);
    }

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    /** Default per-query memory limit for Arrow allocations (256 MB). */
    private static final long DEFAULT_PER_QUERY_MEMORY_LIMIT = 256L * 1024 * 1024;

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final long perQueryMemoryLimit;
    private final List<AnalyticsOperationListener> operationListeners;
    private volatile BufferAllocator bufferAllocator;
    private boolean closed;  // guarded by `this`

    public QueryContext(QueryDAG dag, Executor searchExecutor, AnalyticsQueryTask parentTask) {
        this(dag, searchExecutor, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, DEFAULT_PER_QUERY_MEMORY_LIMIT, List.of());
    }

    public QueryContext(QueryDAG dag, Executor searchExecutor, AnalyticsQueryTask parentTask, int maxConcurrentShardRequests) {
        this(dag, searchExecutor, parentTask, maxConcurrentShardRequests, DEFAULT_PER_QUERY_MEMORY_LIMIT, List.of());
    }

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit
    ) {
        this(dag, searchExecutor, parentTask, maxConcurrentShardRequests, perQueryMemoryLimit, List.of());
    }

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        long perQueryMemoryLimit,
        List<AnalyticsOperationListener> operationListeners
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.perQueryMemoryLimit = perQueryMemoryLimit;
        this.operationListeners = operationListeners;
    }

    public QueryDAG dag() {
        return dag;
    }

    public Executor searchExecutor() {
        return searchExecutor;
    }

    public AnalyticsQueryTask parentTask() {
        return parentTask;
    }

    public String queryId() {
        return dag.queryId();
    }

    public int maxConcurrentShardRequests() {
        return maxConcurrentShardRequests;
    }

    /** Returns the operation listeners for this query. */
    public List<AnalyticsOperationListener> operationListeners() {
        return operationListeners;
    }

    /**
     * Returns the per-query Arrow buffer allocator, creating it lazily on first access.
     * The allocator is a child of the shared root with a per-query memory limit.
     * When the limit is exceeded, Arrow throws {@code OutOfMemoryException} which
     * the stage catches and transitions to FAILED.
     */
    public BufferAllocator bufferAllocator() {
        BufferAllocator alloc = bufferAllocator;
        if (alloc == null) {
            synchronized (this) {
                alloc = bufferAllocator;
                if (alloc == null) {
                    if (closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    alloc = ArrowAllocatorProvider.newChildAllocator("query-" + dag.queryId(), perQueryMemoryLimit);
                    bufferAllocator = alloc;
                }
            }
        }
        return alloc;
    }

    /**
     * Closes the per-query buffer allocator if it was created. Idempotent and
     * serialized with {@link #bufferAllocator()} so close can't race with lazy
     * creation. After close, subsequent {@link #bufferAllocator()} calls throw
     * rather than silently creating a second allocator.
     */
    public void closeBufferAllocator() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            if (bufferAllocator != null) {
                long allocated = bufferAllocator.getAllocatedMemory();
                try {
                    bufferAllocator.close();
                } catch (IllegalStateException e) {
                    // Arrow's close() throws on leak. This exception is attached as a
                    // Suppressed on the already-failing query error and frequently swallowed
                    // by the listener chain. Log + latch on LEAK_SIGNAL so tests can observe
                    // it out-of-band even when the Suppressed is lost.
                    logger.error("[QueryContext] Arrow allocator leaked {} bytes on close: {}", allocated, e.getMessage(), e);
                    LEAK_SIGNAL.compareAndSet(null, "leaked " + allocated + " bytes: " + e.getMessage());
                    assert false : "Arrow allocator leaked " + allocated + " bytes — tighten the failure-path cleanup: " + e.getMessage();
                    throw e;
                }
                bufferAllocator = null;
            }
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return new QueryContext(dag, Runnable::run, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, Long.MAX_VALUE);
    }

    /** Creates a test context with a stub DAG. */
    public static QueryContext forTest(String queryId, AnalyticsQueryTask parentTask) {
        return new QueryContext(
            new QueryDAG(queryId, null),
            Runnable::run,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            Long.MAX_VALUE
        );
    }
}
