/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.analytics.backend.LocalStageContext;
import org.opensearch.analytics.backend.LocalStageRequest;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only {@link LocalStageContext} that records all fed responses per child
 * and produces a summary output on asyncFinalize. Replaces the old
 * {@code TestSummingLocalExecEngine}.
 *
 * <p>Used by {@code LocalStageDispatchIT} to verify the local stage dispatch
 * plumbing without requiring a real DataFusion backend.
 */
public class TestSummingLocalStageContext implements LocalStageContext {

    private final Map<Integer, List<FragmentExecutionResponse>> received = new ConcurrentHashMap<>();
    private final ExchangeSink downstream;
    private final AtomicInteger closeCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Thread drainThread;

    public TestSummingLocalStageContext(LocalStageRequest req) {
        this.downstream = req.getDownstream();
        for (Integer childId : req.getChildSchemas().keySet()) {
            received.put(childId, new CopyOnWriteArrayList<>());
        }
    }

    @Override
    public ExchangeSink sinkFor(int childStageId) {
        List<FragmentExecutionResponse> bucket = received.get(childStageId);
        if (bucket == null) {
            throw new IllegalArgumentException("No sink registered for child stage " + childStageId);
        }
        return new ExchangeSink() {
            @Override
            public void feed(FragmentExecutionResponse response) {
                bucket.add(response);
            }

            @Override
            public void close() {}

            @Override
            public Iterable<Object[]> readResult() {
                return List.of();
            }

            @Override
            public long getRowCount() {
                return 0;
            }

            @Override
            public Object getValueAt(String column, int rowIndex) {
                return null;
            }
        };
    }

    @Override
    public void asyncFinalize(ActionListener<Void> listener) {
        Thread.ofVirtual().name("test-local-drain").start(() -> {
            drainThread = Thread.currentThread();
            try {
                // Build summary response
                int totalBatches = totalBatchesReceived();
                int totalRows = 0;
                for (List<FragmentExecutionResponse> batches : received.values()) {
                    for (FragmentExecutionResponse resp : batches) {
                        totalRows += resp.getRows().size();
                    }
                }
                int numInputs = received.size();
                int finalTotalRows = totalRows;

                List<Object[]> summaryRows = new ArrayList<>();
                summaryRows.add(new Object[] { (long) numInputs, (long) totalBatches, (long) finalTotalRows });
                FragmentExecutionResponse summary = new FragmentExecutionResponse(
                    List.of("num_inputs", "total_batches", "total_rows"),
                    summaryRows
                );
                downstream.feed(summary);
                close();
                listener.onResponse(null);
            } catch (Exception e) {
                close();
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeCount.incrementAndGet();
        }
    }

    // ---- Test inspection methods ----

    public int totalBatchesReceived() {
        return received.values().stream().mapToInt(List::size).sum();
    }

    public int batchesForInput(String stageInputId) {
        // Map from __stage_N_input__ convention to child stage ID
        for (Map.Entry<Integer, List<FragmentExecutionResponse>> entry : received.entrySet()) {
            String expectedId = "__stage_" + entry.getKey() + "_input__";
            if (expectedId.equals(stageInputId)) {
                return entry.getValue().size();
            }
        }
        return 0;
    }

    public boolean allInputsClosed() {
        // In the new SPI, inputs are closed by asyncFinalize, not individually
        return closed.get();
    }

    public int closeCount() {
        return closeCount.get();
    }

    public Thread drainThread() {
        return drainThread;
    }

    /** No-op — the new SPI stores row-based responses, not Arrow batches. */
    public void releaseAllBatches() {
        // nothing to release
    }
}
