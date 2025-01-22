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
import org.opensearch.arrow.StreamProducer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 *
 */
public class DataFrameStreamProducer {

    public static StreamProducer query(List<byte[]> tickets) {
        return new DataFrameFlightProducer(() -> DataFusion.query(tickets));
    }

    public static StreamProducer join(List<byte[]> left, List<byte[]> right, String joinField) {
        return new DataFrameFlightProducer(() -> DataFusion.join(left, right, joinField));
    }

    static class DataFrameFlightProducer implements StreamProducer {

        private DataFrame df;
        private RecordBatchStream recordBatchStream;

        public DataFrameFlightProducer(Supplier<CompletableFuture<DataFrame>> frameSupplier) {
            this.df = frameSupplier.get().join();
        }

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            System.out.println("Fetching the record batch");
            recordBatchStream = df.getStream(allocator).join();
            VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
            System.out.println(vectorSchemaRoot);
            return vectorSchemaRoot;
        }

        @Override
        public BatchedJob createJob(BufferAllocator allocator) {
            System.out.println("Creating batch job for the stream");
            assert recordBatchStream != null;
            return new BatchedJob() {

                @Override
                public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                    try {
                        while (recordBatchStream.loadNextBatch().join()) {
                            System.out.println(recordBatchStream.getVectorSchemaRoot().getRowCount());
                            // wait for a signal to load the next batch
                            flushSignal.awaitConsumption(1000);
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
                    }
                    ;
                    df.close();
                }
            };
        }
    }
}
