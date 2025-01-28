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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.spi.PartitionedStreamProducer;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 */
public class DataFrameStreamProducer implements PartitionedStreamProducer {

    public static Logger logger = LogManager.getLogger(DataFrameStreamProducer.class);
    private final Function<StreamTicket, CompletableFuture<DataFrame>> frameSupplier;

    private StreamTicket rootTicket;
    private Set<StreamTicket> partitions;
    VectorSchemaRoot root;

    public DataFrameStreamProducer(Function<StreamProducer, StreamTicket> streamRegistrar, Set<StreamTicket> partitions, Function<StreamTicket, CompletableFuture<DataFrame>> frameSupplier) {
        logger.info("Constructed DataFrameFlightProducer");
        this.frameSupplier = frameSupplier;
        this.partitions = partitions;
        this.rootTicket = streamRegistrar.apply(this);
    }

    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) {
        // TODO: Read this from one of the producers?
        Map<String, Field> arrowFields = new HashMap<>();
        Field countField = new Field("count", FieldType.nullable(new ArrowType.Int(64, false)), null);
        arrowFields.put("count", countField);
        arrowFields.put("ord", new Field("ord", FieldType.nullable(new ArrowType.Utf8()), null));
        Schema schema = new Schema(arrowFields.values());
        root = VectorSchemaRoot.create(schema, allocator);
        return root;
    }

    @Override
    public BatchedJob createJob(BufferAllocator allocator) {
        return new BatchedJob() {

            private DataFrame df;
            private RecordBatchStream recordBatchStream;

            @Override
            public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                try {
                    assert rootTicket != null;
                    df = frameSupplier.apply(rootTicket).join();
                    recordBatchStream = df.getStream(allocator, root).get();
                    while (recordBatchStream.loadNextBatch().join()) {
//                        logger.info(recordBatchStream.getVectorSchemaRoot().getRowCount());
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

            @Override
            public boolean isCancelled() {
                return false;
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

    }

    @Override
    public Set<StreamTicket> partitions() {
        return partitions;
    }

    public StreamTicket getRootTicket() {
        return rootTicket;
    }
}
