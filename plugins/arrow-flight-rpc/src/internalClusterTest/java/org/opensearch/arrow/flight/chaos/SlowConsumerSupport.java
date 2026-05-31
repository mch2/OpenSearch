/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.chaos;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;

import java.io.IOException;
import java.util.List;

/**
 * Shared test action used by the slow-consumer ITs. Producer side allocates each batch's
 * buffers through a child of the framework's FLIGHT pool, then calls
 * {@code channel.sendResponseBatch(new SlowConsumerResponse(root))}.
 *
 * <p>Behaviour under load is determined by the producer the cluster boots with:
 * <ul>
 *   <li>Default {@code ArrowFlightProducer}: producer races ahead → buffers pile up in the
 *       eventloop's queue → flight-pool {@code OutOfMemoryException} when the cap is small.</li>
 *   <li>{@code BackpressureArrowFlightProducer}: producer parks on
 *       {@code awaitReadyOrThrow} once gRPC's outbound buffer is full → bounded memory,
 *       producer wall-clock grows under slow consumers but the stream completes.</li>
 * </ul>
 */
public final class SlowConsumerSupport {

    private SlowConsumerSupport() {}

    public static final Schema SCHEMA = new Schema(List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    public static class SlowConsumerResponse extends ArrowBatchResponse {
        public SlowConsumerResponse(VectorSchemaRoot root) {
            super(root);
        }

        public SlowConsumerResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class SlowConsumerRequest extends ActionRequest {
        private final int batchCount;
        private final int rowsPerBatch;

        private final long perBatchSleepMillis;

        public SlowConsumerRequest(int batchCount, int rowsPerBatch) {
            this(batchCount, rowsPerBatch, 0L);
        }

        public SlowConsumerRequest(int batchCount, int rowsPerBatch, long perBatchSleepMillis) {
            this.batchCount = batchCount;
            this.rowsPerBatch = rowsPerBatch;
            this.perBatchSleepMillis = perBatchSleepMillis;
        }

        public SlowConsumerRequest(StreamInput in) throws IOException {
            super(in);
            this.batchCount = in.readInt();
            this.rowsPerBatch = in.readInt();
            this.perBatchSleepMillis = in.readLong();
        }

        public int getBatchCount() {
            return batchCount;
        }

        public int getRowsPerBatch() {
            return rowsPerBatch;
        }

        public long getPerBatchSleepMillis() {
            return perBatchSleepMillis;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(batchCount);
            out.writeInt(rowsPerBatch);
            out.writeLong(perBatchSleepMillis);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class SlowConsumerAction extends ActionType<SlowConsumerResponse> {
        public static final SlowConsumerAction INSTANCE = new SlowConsumerAction();
        public static final String NAME = "cluster:internal/test/slow_consumer";

        private SlowConsumerAction() {
            super(NAME, SlowConsumerResponse::new);
        }
    }

    /**
     * Server-side action: emits {@code request.batchCount} batches as fast as it can.
     * Under the default producer with a tight flight-pool cap, this hits OOM at
     * {@code VectorSchemaRoot.create}. Under the back-pressure producer, the call to
     * {@code channel.sendResponseBatch} parks the producer thread until gRPC drains.
     */
    public static class TransportSlowConsumerAction extends TransportAction<SlowConsumerRequest, SlowConsumerResponse> {
        private final BufferAllocator allocator;

        @Inject
        public TransportSlowConsumerAction(
            StreamTransportService streamTransportService,
            ActionFilters actionFilters,
            ArrowNativeAllocator nativeAllocator
        ) {
            super(SlowConsumerAction.NAME, actionFilters, streamTransportService.getTaskManager());
            this.allocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT)
                .newChildAllocator("slow-consumer-it", 0, Long.MAX_VALUE);
            streamTransportService.registerRequestHandler(
                SlowConsumerAction.NAME,
                ThreadPool.Names.GENERIC,
                SlowConsumerRequest::new,
                this::handleStreamRequest
            );
        }

        @Override
        protected void doExecute(Task task, SlowConsumerRequest request, ActionListener<SlowConsumerResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
        }

        private void handleStreamRequest(SlowConsumerRequest request, TransportChannel channel, Task task) throws IOException {
            try {
                for (int b = 0; b < request.getBatchCount(); b++) {
                    if (request.getPerBatchSleepMillis() > 0) {
                        // Simulates per-batch compute so the producer's allocation rate is
                        // comparable to gRPC's drain rate; without it a tight loop can fill
                        // the eventloop's unbounded queue before isReady() flips false.
                        try {
                            Thread.sleep(request.getPerBatchSleepMillis());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted", e);
                        }
                    }
                    VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
                    boolean transferred = false;
                    try {
                        IntVector v = (IntVector) root.getVector("value");
                        v.allocateNew(request.getRowsPerBatch());
                        for (int i = 0; i < request.getRowsPerBatch(); i++) {
                            v.setSafe(i, i);
                        }
                        root.setRowCount(request.getRowsPerBatch());
                        channel.sendResponseBatch(new SlowConsumerResponse(root));
                        transferred = true;
                    } finally {
                        // Either we handed ownership to the framework or we own the partially-built
                        // root and must release it. Without this, an OOM mid-allocation leaks the
                        // schema/metadata buffers VectorSchemaRoot.create allocated.
                        if (!transferred) {
                            root.close();
                        }
                    }
                }
                channel.completeStream();
            } catch (Exception e) {
                channel.sendResponse(e);
            }
        }
    }

    public static class SlowConsumerTestPlugin extends Plugin implements ActionPlugin {
        public SlowConsumerTestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(SlowConsumerAction.INSTANCE, TransportSlowConsumerAction.class));
        }
    }
}
