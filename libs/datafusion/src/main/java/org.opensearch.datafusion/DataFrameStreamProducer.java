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
import org.opensearch.arrow.StreamProducer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 *
 */
public class DataFrameStreamProducer {

    public static StreamProducer query(byte[] ticket) {
        return new DataFrameFlightProducer(() -> DataFusion.query(ticket));
    }

    public static StreamProducer agg(byte[] ticket) {
        return new DataFrameFlightProducer(() -> DataFusion.agg(ticket));
    }

    public static StreamProducer join(byte[] left, byte[] right, String joinField) {
        return new DataFrameFlightProducer(() -> DataFusion.join(left, right, joinField));
    }

    static class DataFrameFlightProducer implements StreamProducer {

        private DataFrame df;
        private RecordBatchStream recordBatchStream;

        public DataFrameFlightProducer(Supplier<CompletableFuture<DataFrame>> frameSupplier) {
            this.df = frameSupplier.get().join();
            logger.info("Constructed DataFrameFlightProducer");
        }

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            logger.info("Fetching the record batch");
            recordBatchStream = df.getStream(allocator).join();
            logger.info("Finished fetching the record batch");
            VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
            logger.info("Returning VectorSchemaRoot");
            return vectorSchemaRoot;
        }
        public static Logger logger = LogManager.getLogger(DataFrameStreamProducer.class);

        @Override
        public BatchedJob createJob(BufferAllocator allocator) {
            logger.info("createJob");
            assert recordBatchStream != null;
            return new BatchedJob() {

                @Override
                public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                    try {
                        logger.info("Loading next batch");
                        while (recordBatchStream.loadNextBatch().join()) {
                            logger.info(recordBatchStream.getVectorSchemaRoot().getRowCount());
                            // wait for a signal to load the next batch
                            logger.info("Awaiting consumption at coordinator");
                            flushSignal.awaitConsumption(1000);
                            logger.info("Consumed batch at coord");
                        }
                        close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onCancel() {
                    try {
                        close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                void close() throws Exception {
                    if (recordBatchStream != null) {
                        recordBatchStream.close();
                    };
                    df.close();
                }
            };
        }
    }
}
