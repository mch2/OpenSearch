/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.batchoperationlistener.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates all batch operation listener stats
 *
 * @opensearch.internal
 */
public class BatchOperationListenerInternalStats implements Writeable {

    /** Number of times the operation queue threshold was reached causing rejections */
    private final AtomicLong operationQueueLimitReached;

    /** # of operations drained from the queue. */
    private final AtomicLong drainQueueCount;

    /** # of timeouts waiting for drain latch. */
    private final AtomicLong drainQueueTimeout;

    /** # of timeouts waiting for sink to ack. */
    private final AtomicLong pollOperationTimeout;

    /** The total number of request processed. */
    private final AtomicLong totalRequestsProcessed;

    /** The total number of requests that failed processing. */
    private final AtomicLong totalRequestsFailed;

    /** The total latency of the listener (in milliseconds). */
    private final AtomicLong totalTimeInMillis;

    public BatchOperationListenerInternalStats() {
        this.operationQueueLimitReached = new AtomicLong(0);
        this.drainQueueCount = new AtomicLong(0);
        this.drainQueueTimeout = new AtomicLong(0);
        this.pollOperationTimeout = new AtomicLong(0);
        this.totalRequestsProcessed = new AtomicLong(0);
        this.totalRequestsFailed = new AtomicLong(0);
        this.totalTimeInMillis = new AtomicLong(0);
    }

    public BatchOperationListenerInternalStats(StreamInput in) throws IOException {
        this.operationQueueLimitReached = new AtomicLong(in.readLong());
        this.drainQueueCount = new AtomicLong(in.readLong());
        this.drainQueueTimeout = new AtomicLong(in.readLong());
        this.pollOperationTimeout = new AtomicLong(in.readLong());
        this.totalRequestsProcessed = new AtomicLong(in.readLong());
        this.totalRequestsFailed = new AtomicLong(in.readLong());
        this.totalTimeInMillis = new AtomicLong(in.readLong());
    }

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field(Fields.OPERATION_QUEUE_LIMIT_REACHED, operationQueueLimitReached.get());
        builder.field(Fields.DRAIN_QUEUE_COUNT, drainQueueCount.get());
        builder.field(Fields.DRAIN_QUEUE_TIMEOUT, drainQueueTimeout.get());
        builder.field(Fields.POLL_OPERATION_TIMEOUT, pollOperationTimeout.get());
        builder.field(Fields.TOTAL_REQUESTS_PROCESSED, totalRequestsProcessed.get());
        builder.field(Fields.TOTAL_REQUESTS_FAILED, totalRequestsFailed.get());
        builder.field(Fields.TIME, totalTimeInMillis.get());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(operationQueueLimitReached.get());
        out.writeLong(drainQueueCount.get());
        out.writeLong(drainQueueTimeout.get());
        out.writeLong(pollOperationTimeout.get());
        out.writeLong(totalRequestsProcessed.get());
        out.writeLong(totalRequestsFailed.get());
        out.writeLong(totalTimeInMillis.get());
    }

    public void incrementDrainQueueTimeout() {
        drainQueueTimeout.incrementAndGet();
    }

    public void incrementPollOperationTimeout() {
        pollOperationTimeout.incrementAndGet();
    }

    public void addFailedRequestCount(long numOfFailedRequests) {
        totalRequestsFailed.addAndGet(numOfFailedRequests);
    }

    public void addTotalRequestTimeInMillis(long time) {
        totalTimeInMillis.addAndGet(time);
    }

    public void addRequestsProcessed(long requestsProcessed) {
        totalRequestsProcessed.addAndGet(requestsProcessed);
    }

    public void addDrainQueueCount(long count) {
        drainQueueCount.addAndGet(count);
    }

    public void incrementOperationQueueLimitReached() {
        operationQueueLimitReached.incrementAndGet();
    }

    public AtomicLong getOperationQueueLimitReached() {
        return operationQueueLimitReached;
    }

    public AtomicLong getDrainQueueCount() {
        return drainQueueCount;
    }

    public AtomicLong getPollOperationTimeout() {
        return pollOperationTimeout;
    }

    public AtomicLong getDrainQueueTimeout() {
        return drainQueueTimeout;
    }

    public AtomicLong getTotalRequestsProcessed() {
        return totalRequestsProcessed;
    }

    public AtomicLong getTotalRequestsFailed() {
        return totalRequestsFailed;
    }

    public AtomicLong getTotalTimeInMillis() {
        return totalTimeInMillis;
    }

    static final class Fields {
        static final String BATCH_OPERATION_LISTENER = "batch_operation_listener";
        static final String OPERATION_QUEUE_SIZE = "operation_queue_size";
        static final String OPERATION_QUEUE_LIMIT_REACHED = "operation_queue_limit_breach";
        static final String DRAIN_QUEUE_COUNT = "drain_queue_count";
        static final String DRAIN_QUEUE_TIMEOUT = "drain_queue_timeout";
        static final String POLL_OPERATION_TIMEOUT = "poll_operation_timeout";
        static final String TOTAL_REQUESTS_PROCESSED = "total_requests_processed";
        static final String TOTAL_REQUESTS_FAILED = "total_requests_failed";
        static final String TIME = "total_time";
    }
}
