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
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mutable per-query shared state. Separate from {@link QueryContext}
 * (immutable config) so components that read config don't have access to
 * mutate state by accident.
 *
 * <p>Holds:
 * <ul>
 *   <li>{@link #rootSink()} — per-query result accumulator (lazy)</li>
 *   <li>{@link #completedStages()} — stages that have finished</li>
 *   <li>{@link #shuffleManifests()} — partition manifests from shuffle-write stages</li>
 * </ul>
 *
 * <p>Transport concurrency state ({@code pendingPerNode}) lives inside
 * {@link Scheduler#execute}, not here, because it's a transport-layer concern
 * that doesn't affect query semantics.
 *
 * @opensearch.internal
 */
public class QueryState {

    private volatile ExchangeSink rootSink;
    private volatile BufferAllocator bufferAllocator;
    private final Set<Integer> completedStages = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests = new ConcurrentHashMap<>();
    private final Map<Integer, StageExecution> stageExecutions = new ConcurrentHashMap<>();
    private final Map<Integer, StageMetrics> stageMetrics = new ConcurrentHashMap<>();

    public QueryState() {}

    /** Test constructor that injects an explicit sink (for tests that inspect sink state). */
    QueryState(ExchangeSink rootSink) {
        this.rootSink = rootSink;
    }

    /**
     * Returns the root sink, creating it lazily on first access.
     * MVP: {@link SimpleExchangeSink}. Future: backend-provided sink for local stage reduction.
     */
    public ExchangeSink rootSink() {
        ExchangeSink sink = rootSink;
        if (sink == null) {
            synchronized (this) {
                sink = rootSink;
                if (sink == null) {
                    sink = new SimpleExchangeSink();
                    rootSink = sink;
                }
            }
        }
        return sink;
    }

    public Set<Integer> completedStages() {
        return completedStages;
    }

    /**
     * Returns the per-query Arrow buffer allocator, creating it lazily on first access.
     * The allocator is a child of a root allocator and should be closed when the query completes.
     */
    public BufferAllocator bufferAllocator() {
        BufferAllocator alloc = bufferAllocator;
        if (alloc == null) {
            synchronized (this) {
                alloc = bufferAllocator;
                if (alloc == null) {
                    alloc = new RootAllocator(Long.MAX_VALUE);
                    bufferAllocator = alloc;
                }
            }
        }
        return alloc;
    }

    /**
     * Closes the per-query buffer allocator if it was created.
     * Called by the plan executor when the query completes.
     */
    public void closeBufferAllocator() {
        BufferAllocator alloc = bufferAllocator;
        if (alloc != null) {
            alloc.close();
        }
    }

    public Map<Integer, Map<ShardId, Map<Integer, String>>> shuffleManifests() {
        return shuffleManifests;
    }

    /**
     * Returns the {@link StageMetrics} for the given stage, creating one on
     * first access. The same instance is returned on subsequent calls for the
     * same stageId, making this the single source of truth for per-stage metrics.
     */
    public StageMetrics metricsFor(int stageId) {
        return stageMetrics.computeIfAbsent(stageId, StageMetrics::new);
    }

    /**
     * Returns an unmodifiable snapshot of all per-stage metrics created so far.
     * Entries live as long as this {@code QueryState} instance.
     */
    public Map<Integer, StageMetrics> allStageMetrics() {
        return Collections.unmodifiableMap(stageMetrics);
    }

    /**
     * Registers a stage execution so that query-level controllers (cancellation,
     * future AQE / observability) can discover it while it is in flight.
     */
    public void registerStageExecution(StageExecution exec) {
        stageExecutions.put(exec.getStageId(), exec);
    }

    /**
     * Removes a stage execution after it reaches a terminal state.
     * Idempotent — removing a non-existent key is a no-op.
     */
    public void unregisterStageExecution(int stageId) {
        stageExecutions.remove(stageId);
    }

    /**
     * Returns a snapshot of the currently registered (in-flight) stage executions.
     * The returned list is a copy; mutations to it do not affect the registry.
     */
    public Collection<StageExecution> activeStageExecutions() {
        return new ArrayList<>(stageExecutions.values());
    }
}
