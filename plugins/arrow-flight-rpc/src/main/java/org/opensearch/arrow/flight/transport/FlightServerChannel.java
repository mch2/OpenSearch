/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.arrow.flight.transport.FlightErrorMapper.mapFromCallStatus;

/**
 * TcpChannel implementation for Arrow Flight. It is created per call in ArrowFlightProducer.
 * This implementation is not thread safe; consumer must ensure to invoke sendBatch serially and call completeStream() at the end.
 *
 * <p><b>Schema frame contract:</b> the Arrow Flight wire protocol delivers the schema as the first DATA frame on the stream
 * (emitted by {@link org.apache.arrow.flight.FlightProducer.ServerStreamListener#start} via the first
 * {@link #sendBatch}). Calling {@link #completeStream} without any prior {@link #sendBatch} writes only the response header
 * and closes the stream — no schema frame is ever transmitted, and the client's {@code FlightStream.next()} will block
 * until the call timeout. Callers MUST emit at least one batch (it may be a zero-row {@link VectorSchemaRoot} carrying
 * just the schema) before completing the stream. Error completion via {@link #sendError} is exempt because the error
 * itself terminates the client wait.
 */
class FlightServerChannel implements TcpChannel, ArrowFlightChannel {
    private static final String PROFILE_NAME = "flight";

    private final Logger logger = LogManager.getLogger(FlightServerChannel.class);
    private final ServerStreamListener serverStreamListener;
    private final BufferAllocator allocator;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private final List<ActionListener<Void>> closeListeners = Collections.synchronizedList(new ArrayList<>());
    private final ServerHeaderMiddleware middleware;
    private volatile VectorSchemaRoot root = null;
    private final FlightCallTracker callTracker;
    private volatile boolean cancelled = false;
    private final ExecutorService executor;
    private final long correlationId;
    private final AtomicInteger batchNumber = new AtomicInteger(0);

    public FlightServerChannel(
        ServerStreamListener serverStreamListener,
        BufferAllocator allocator,
        ServerHeaderMiddleware middleware,
        FlightCallTracker callTracker,
        ExecutorService executor
    ) {
        this.correlationId = Long.parseLong(middleware.getCorrelationId());
        logger.debug("Creating FlightServerChannel for correlation ID: {}", correlationId);
        this.serverStreamListener = serverStreamListener;
        this.serverStreamListener.setUseZeroCopy(true);
        this.serverStreamListener.setOnCancelHandler(() -> {
            cancelled = true;
            callTracker.recordCallEnd(StreamErrorCode.CANCELLED.name());
            close();
        });
        this.allocator = allocator;
        this.middleware = middleware;
        this.callTracker = callTracker;
        this.executor = executor;
        this.localAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        this.remoteAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    VectorSchemaRoot getRoot() {
        return root;
    }

    /**
     * Gets the executor for this channel
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Sends a batch of data as a VectorSchemaRoot.
     *
     * @param output StreamOutput for the response
     */
    public void sendBatch(ByteBuffer header, VectorStreamOutput output) {
        if (cancelled) {
            throw StreamException.cancelled("Cannot flush more batches. Stream cancelled by the client");
        }
        if (!open.get()) {
            throw new IllegalStateException("FlightServerChannel already closed.");
        }
        batchNumber.incrementAndGet();
        long batchStartTime = System.nanoTime();
        // Only set for the first batch
        if (root == null) {
            middleware.setHeader(header);
            root = output.getRoot();
            serverStreamListener.start(root);
        } else {
            root = output.getRoot();
            // placeholder to clear and fill the root with data for the next batch
        }
        logger.debug("Sending batch #{} for correlation ID: {}", batchNumber, correlationId);
        // we do not want to close the root right after putNext() call as we do not know the status of it whether
        // its transmitted at transport; we close them all at complete stream. TODO: optimize this behaviour
        serverStreamListener.putNext();
        long putNextTime = (System.nanoTime() - batchStartTime) / 1_000_000;
        if (callTracker != null) {
            long rootSize = FlightUtils.calculateVectorSchemaRootSize(root);
            callTracker.recordBatchSent(rootSize, System.nanoTime() - batchStartTime);
            logger.debug(
                "Batch #{} sent for correlation ID: {}, size: {} bytes, putNext: {}ms",
                batchNumber,
                correlationId,
                rootSize,
                putNextTime
            );
        } else {
            logger.debug("Batch #{} sent for correlation ID: {}, putNext: {}ms", batchNumber, correlationId, putNextTime);
        }
    }

    /**
     * Completes the streaming response and closes all pending roots.
     *
     * <p><b>MUST be preceded by at least one {@link #sendBatch} call.</b> The Arrow Flight schema frame is emitted by
     * {@link org.apache.arrow.flight.FlightProducer.ServerStreamListener#start} on the first {@code sendBatch}; if no
     * batch has been sent ({@link #root} is still {@code null}), only the response header is written here and the
     * client's {@code FlightStream.next()} will block waiting for a schema frame that never arrives, ultimately
     * timing out. If a producer can legitimately yield zero data batches (e.g. a filter excludes all rows), it must
     * still emit a schema-bearing zero-row batch before calling this method. See
     * {@code DatafusionResultStream.BatchIterator} for an example of this synthesis.
     */
    public void completeStream(ByteBuffer header) {
        try {
            if (!open.get()) {
                throw new IllegalStateException("FlightServerChannel already closed.");
            }
            if (root == null) {
                // No batches sent — schema frame was never emitted. The response header is written but the client
                // will block on FlightStream.next() until timeout. This branch exists for backward compatibility
                // with callers that may complete without batches (notably error fallbacks after sendResponse(e)
                // failure); new callers should always send at least one schema-bearing batch first.
                middleware.setHeader(header);
                logger.warn(
                    "Completing Flight stream for correlation ID: {} with no batches sent — schema frame was never "
                        + "transmitted; client will block until timeout. Caller must emit at least one (possibly "
                        + "zero-row) batch before completeStream.",
                    correlationId
                );
            } else {
                logger.debug("Completing stream for correlation ID: {} after {} batches", correlationId, batchNumber);
            }
            serverStreamListener.completed();
        } finally {
            callTracker.recordCallEnd(StreamErrorCode.OK.name());
        }
    }

    /**
     * Sends an error and closes the channel.
     *
     * @param error the error to send
     */
    public void sendError(ByteBuffer header, Exception error) {
        FlightRuntimeException flightExc = null;
        try {
            if (!open.get()) {
                throw new IllegalStateException("FlightServerChannel already closed.");
            }
            if (error instanceof FlightRuntimeException fre) {
                flightExc = fre;
            } else {
                flightExc = CallStatus.INTERNAL.withCause(error)
                    .withDescription(error.getMessage() != null ? error.getMessage() : "Stream error")
                    .toRuntimeException();
            }
            middleware.setHeader(header);
            if (error instanceof OpenSearchException) {
                logger.debug("Error in Flight stream: {}", error.getMessage());
            } else {
                logger.error("Unexpected error in Flight stream", error);
            }
            logger.debug("Sending error for correlation ID: {} after {} batches: {}", correlationId, batchNumber, error.getMessage());
            serverStreamListener.error(flightExc);
        } finally {
            StreamErrorCode errorCode = flightExc != null ? mapFromCallStatus(flightExc) : StreamErrorCode.UNKNOWN;
            callTracker.recordCallEnd(errorCode.name());
        }
    }

    @Override
    public boolean isServerChannel() {
        return true;
    }

    @Override
    public String getProfile() {
        return PROFILE_NAME;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        listener.onFailure(new UnsupportedOperationException("FlightServerChannel does not support BytesReference based sendMessage()"));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        // Assume Arrow Flight is connected
        listener.onResponse(null);
    }

    @Override
    public ChannelStats getChannelStats() {
        return new ChannelStats(); // TODO: Implement stats. Add custom stats as needed
    }

    @Override
    public void close() {
        if (!open.get()) {
            return;
        }
        open.set(false);
        if (root != null) {
            root.close();
        }
        notifyCloseListeners();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        if (!open.get()) {
            listener.onResponse(null);
        } else {
            closeListeners.add(listener);
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    private void notifyCloseListeners() {
        for (ActionListener<Void> listener : closeListeners) {
            listener.onResponse(null);
        }
        closeListeners.clear();
    }
}
