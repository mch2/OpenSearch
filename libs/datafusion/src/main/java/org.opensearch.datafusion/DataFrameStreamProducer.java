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
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 */
public class DataFrameStreamProducer implements StreamProducer {

    public static Logger logger = LogManager.getLogger(DataFrameStreamProducer.class);
    private final Function<StreamTicket, CompletableFuture<DataFrame>> frameSupplier;
    private final Function<StreamProducer, StreamTicket> streamRegistrar;

    private StreamTicket rootTicket;
    private Set<StreamTicket> partitions;
    VectorSchemaRoot root;
    private DataFrame df;
    private RecordBatchStream recordBatchStream;

    public DataFrameStreamProducer(Function<StreamProducer, StreamTicket> streamRegistrar, Set<StreamTicket> partitions, Function<StreamTicket, CompletableFuture<DataFrame>> frameSupplier) {
        this.streamRegistrar = streamRegistrar;
        this.frameSupplier = frameSupplier;
        this.partitions = partitions;
        this.rootTicket = streamRegistrar.apply(this);
    }

    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) {
        if (recordBatchStream == null || df == null) {
            this.df = frameSupplier.apply(rootTicket).join();
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
                    pollUntilFalse(() -> recordBatchStream.loadNextBatch(), flushSignal);
                    close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private CompletableFuture<Void> pollUntilFalse(
                Supplier<CompletableFuture<Boolean>> pollingFunction, FlushSignal signal) {
                return pollingFunction.get()
                    .thenCompose(result -> {
                        if (result) {
                            // If true, continue polling
                            signal.awaitConsumption(TimeValue.timeValueMillis(1000));
                            return pollUntilFalse(pollingFunction, signal);
                        } else {
                            // If false, stop polling
                            return CompletableFuture.completedFuture(null);
                        }
                    });
            }

            @Override
            public void onCancel() {
                try {
                    close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            void close() throws Exception {
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
        if (recordBatchStream != null) {
            try {
                recordBatchStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            df.close();
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
