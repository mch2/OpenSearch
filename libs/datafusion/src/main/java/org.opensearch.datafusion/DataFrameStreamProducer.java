/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 */
public class DataFrameStreamProducer implements StreamProducer {

    public static Logger logger = LogManager.getLogger(DataFrameStreamProducer.class);
    private final BiFunction<SessionContext, StreamTicket, CompletableFuture<DataFrame>> frameSupplier;
    private final StreamManager streamRegistrar;

    private StreamTicket rootTicket;
    private Set<StreamTicket> partitions;
    VectorSchemaRoot root;
    private DataFrame df;
    private RecordBatchStream recordBatchStream;
    private boolean isCancelled = false;
    SessionContext ctx;
    public DataFrameStreamProducer(StreamManager streamRegistrar,
                                   Set<StreamTicket> partitions,
                                   BiFunction<SessionContext, StreamTicket, CompletableFuture<DataFrame>> frameSupplier) {
        this.ctx = new SessionContext();
        this.streamRegistrar = streamRegistrar;
        this.frameSupplier = frameSupplier;
        this.partitions = partitions;
        this.rootTicket = streamRegistrar.registerStream(this, TaskId.EMPTY_TASK_ID);
        logger.info("Registering stream producer {}", rootTicket);
    }

    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) {
        if (recordBatchStream == null || df == null) {
            this.df = frameSupplier.apply(ctx, rootTicket).join();
            try {
                this.recordBatchStream = df.getStream(allocator).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error creating root");
                throw new RuntimeException(e);
            }
        }
        return recordBatchStream.getVectorSchemaRoot();
    }

    @Override
    public BatchedJob createJob(BufferAllocator allocator) {
        return new BatchedJob() {

            @Override
            public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                try {
                    assert rootTicket != null;
                    // loadNextBatch will execute async in datafusion
                    while (recordBatchStream.loadNextBatch().join()) {
                        flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onCancel() {
                isCancelled = true;
            }

            @Override
            public boolean isCancelled() {
                return isCancelled;
            }

            void close() throws Exception {
                logger.info("Closing in BatchedJob");
                // close things when the producer does
            }
        };
    }

    @Override
    public int estimatedRowCount() {
        return 0;
    }

    @Override
    public String getAction() {
        return "";
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing in producer");
        if (recordBatchStream != null) {
            try {
                recordBatchStream.close();
            } catch (Exception e) {
                logger.error("Unable to close recordbatchstream", e);
                throw new RuntimeException(e);
            }
        }
        try {
            df.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        streamRegistrar.removeStream(rootTicket);
        try {
            ctx.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<StreamTicket> partitions() {
        return partitions;
    }

    @Override
    public TimeValue getJobDeadline() {
        return TimeValue.timeValueMinutes(5);
    }
}
