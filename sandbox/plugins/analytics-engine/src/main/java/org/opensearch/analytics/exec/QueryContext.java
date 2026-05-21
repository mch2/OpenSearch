/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.QueryDAG;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Per-query context — immutable config (DAG, executor, parent task) + lazy per-query
 * resources (Arrow buffer allocator, virtual-thread executor for LOCAL tasks).
 *
 * @opensearch.internal
 */
public class QueryContext {

    // TODO: make configurable via cluster setting (like search.max_concurrent_shard_requests)
    private static final int DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS = 5;

    /**
     * Per-query cap on total in-flight outbound shard requests across all nodes. Composes with
     * the per-node {@link #maxConcurrentShardRequests} throttle — a dispatch acquires both.
     */
    private static final int DEFAULT_MAX_CONCURRENT_OUTBOUND_SHARDS = 50;

    private final QueryDAG dag;
    private final Executor searchExecutor;
    private final AnalyticsQueryTask parentTask;
    private final int maxConcurrentShardRequests;
    private final List<AnalyticsOperationListener> operationListeners;
    private final BufferAllocator allocator;
    private final boolean ownsAllocator;
    /** Per-query total outbound-shard throttle. See {@link #DEFAULT_MAX_CONCURRENT_OUTBOUND_SHARDS}. */
    private final PendingExecutions outboundShardThrottle;
    private volatile ExecutorService localTaskExecutor;
    private boolean closed;  // guarded by `this`

    public QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        BufferAllocator allocator,
        boolean ownsAllocator
    ) {
        this(dag, searchExecutor, parentTask, DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS, List.of(), allocator, ownsAllocator);
    }

    /** Full-parameter constructor. Private; tests use {@link #forTest} factories. */
    private QueryContext(
        QueryDAG dag,
        Executor searchExecutor,
        AnalyticsQueryTask parentTask,
        int maxConcurrentShardRequests,
        List<AnalyticsOperationListener> operationListeners,
        BufferAllocator allocator,
        boolean ownsAllocator
    ) {
        this.dag = dag;
        this.searchExecutor = searchExecutor;
        this.parentTask = parentTask;
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
        this.operationListeners = operationListeners;
        this.allocator = allocator;
        this.ownsAllocator = ownsAllocator;
        this.outboundShardThrottle = new PendingExecutions(DEFAULT_MAX_CONCURRENT_OUTBOUND_SHARDS);
    }

    /**
     * Per-query total outbound-shard throttle. Acquire before each shard dispatch, release on
     * terminal (success or failure). Composes with the per-node {@code maxConcurrentShardRequests}
     * throttle: a dispatch holds both simultaneously.
     */
    public PendingExecutions outboundShardThrottle() {
        return outboundShardThrottle;
    }

    /**
     * Permit count for {@link #outboundShardThrottle()} — also the dispatch window the shard
     * stage uses to size its initial batch when dispatching incrementally. Kept in sync with
     * the throttle by construction; exposed as a method so stages don't have to reach through
     * {@link PendingExecutions} internals.
     */
    public int maxConcurrentOutboundShards() {
        return DEFAULT_MAX_CONCURRENT_OUTBOUND_SHARDS;
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

    public BufferAllocator bufferAllocator() {
        return allocator;
    }

    /** Lazy per-query virtual-thread executor for LOCAL tasks. */
    public ExecutorService localTaskExecutor() {
        ExecutorService exec = localTaskExecutor;
        if (exec == null) {
            synchronized (this) {
                exec = localTaskExecutor;
                if (exec == null) {
                    if (closed) {
                        throw new IllegalStateException("QueryContext closed for query " + dag.queryId());
                    }
                    exec = Executors.newThreadPerTaskExecutor(
                        Thread.ofVirtual().name("analytics-local-task-" + dag.queryId() + "-", 0).factory()
                    );
                    localTaskExecutor = exec;
                }
            }
        }
        return exec;
    }

    boolean ownsAllocator() {
        return ownsAllocator;
    }

    /** Idempotent. Serialised with lazy-init accessors; post-close accessors throw. */
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            if (ownsAllocator) {
                allocator.close();
            }
            if (localTaskExecutor != null) {
                localTaskExecutor.shutdown();
                localTaskExecutor = null;
            }
        }
    }

    // ─── Test factories ────────────────────────────────────────────────

    private static final RootAllocator TEST_ROOT = new RootAllocator(Long.MAX_VALUE);

    /** Creates a test context with a synchronous executor. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask) {
        return forTest(dag, parentTask, List.of());
    }

    /** Creates a test context with a synchronous executor and the supplied operation listeners. */
    public static QueryContext forTest(QueryDAG dag, AnalyticsQueryTask parentTask, List<AnalyticsOperationListener> operationListeners) {
        BufferAllocator testAllocator = TEST_ROOT.newChildAllocator("test-" + dag.queryId(), 0, Long.MAX_VALUE);
        return new QueryContext(
            dag,
            Runnable::run,
            parentTask,
            DEFAULT_MAX_CONCURRENT_SHARD_REQUESTS,
            operationListeners,
            testAllocator,
            true
        );
    }
}
