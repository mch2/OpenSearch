/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream.collector;/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.DataFrame;
import org.opensearch.datafusion.RecordBatchStream;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Arrow collector for OpenSearch fields values
 */
@ExperimentalApi
public class PushStreamingCollector extends FilterCollector {

    public static final String COUNT = "count";
    public static final String ORD = "ord";
    private VectorSchemaRoot collectionRoot;
    private final BufferAllocator allocator;
    List<ArrowFieldAdaptor> fields;
    private final VectorSchemaRoot bucketRoot;
    private final StreamProducer.FlushSignal flushSignal;
    public static Logger logger = LogManager.getLogger(PushStreamingCollector.class);
    // Pre-allocate reusable buffers
    private int batchSize;

    public PushStreamingCollector(
        Collector in,
        DictionaryProvider provider,
        VectorSchemaRoot collectionRoot,
        VectorSchemaRoot root,
        BufferAllocator allocator,
        List<ArrowFieldAdaptor> fields,
        int batchSize,
        StreamProducer.FlushSignal flushSignal,
        ShardId shardId
    ) {
        super(in);
        this.allocator = allocator;
        this.fields = fields;
        this.bucketRoot = root;
        this.collectionRoot = collectionRoot;
        this.flushSignal = flushSignal;
        this.batchSize = batchSize;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        ArrowFieldAdaptor arrowFieldAdaptor = fields.get(0);
        String term = arrowFieldAdaptor.fieldName;
        SortedSetDocValues dv = ((ArrowFieldAdaptor.SortedDocValuesType) arrowFieldAdaptor.getDocValues(context.reader())).getSortedDocValues();
        final int maxOrd = (int) dv.getValueCount();

        // ordinalVector to hold ordinals
        final FieldVector[] ordinalVector = {collectionRoot.getVector(arrowFieldAdaptor.fieldName)};
        // setting initial capacity to maxOrd to skip a few resizes.
        ordinalVector[0].setInitialCapacity(maxOrd);
        DataFusionAggregator aggregator = new DataFusionAggregator(term);
        List<CompletableFuture<DataFrame>> futures = new ArrayList<>();

        final int[] currentRow = {0};
        return new LeafCollector() {

            @Override
            public void collect(int docId) throws IOException {
                if (currentRow[0] >= batchSize) {
                    pushBatch();
                }
                // dump all the ords into an arrow ordinalVector, df will aggregate on these and then decode
                dv.advance(docId);
                final int docValueCount = dv.docValueCount();
                for (int i = 0; i < docValueCount; i++) {
                    long ord = dv.nextOrd();
                    ((UInt8Vector) ordinalVector[0]).setSafe(currentRow[0], ord);
                }
                currentRow[0] += docValueCount;
            }

            private void pushBatch() throws IOException {
                ordinalVector[0].setValueCount(currentRow[0]);
                collectionRoot.setRowCount(currentRow[0]);

                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try {
                        // get a ref to the existing batch to aggregate
                        VectorSchemaRoot batchToProcess = collectionRoot;
                        // create a new vector with the same schema to write new batch into
                        collectionRoot = VectorSchemaRoot.create(batchToProcess.getSchema(), allocator);
                        // get a ref to a new ord vector to fill
                        ordinalVector[0] = collectionRoot.getVector(arrowFieldAdaptor.fieldName);

                        // Push batch to streaming aggregator
                        CompletableFuture<DataFrame> fut = aggregator.pushBatch(allocator, batchToProcess);
                        fut.whenComplete((frame, err) -> {
                            batchToProcess.close();
                        });
                        futures.add(fut);

                        // Only reset after we've set up the new batch
                        currentRow[0] = 0;

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    pushBatch();
                }

                try {
                    // Get final results and process into bucketRoot
                    // ensure all batches have finished being processed in DF
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    List<DataFrame> frames = new ArrayList<>(futures.size());
                    for (CompletableFuture<DataFrame> future : futures) {
                        frames.add(future.join());
                    }

                    DataFrame results = aggregator.getResults(frames, 500).get();
                    if (results != null) {
                        RecordBatchStream recordBatchStream = results.getStream(allocator).get();
                        VectorSchemaRoot root = recordBatchStream.getVectorSchemaRoot();
                        VarCharVector ordVector = (VarCharVector) bucketRoot.getVector(ORD);
                        BigIntVector countVector = (BigIntVector) bucketRoot.getVector(COUNT);
                        int row = 0;
                        while (recordBatchStream.loadNextBatch().join()) {
                            UInt8Vector dfVector = (UInt8Vector) root.getVector(ORD);
                            FieldVector cv = root.getVector(COUNT);
                            for (int i = 0; i < dfVector.getValueCount(); i++) {
                                BytesRef bytesRef = dv.lookupOrd(dfVector.get(i));
                                ordVector.setSafe(row, bytesRef.bytes, 0, bytesRef.length);
                                countVector.setSafe(row, ((BigIntVector) cv).get(i));
                                row++;
                            }
                        }
                        ordVector.setValueCount(row);
                        countVector.setValueCount(row);
                        bucketRoot.setRowCount(row);
                        flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000 * 120));
                        recordBatchStream.close();
                        results.close();
                    }

                    collectionRoot.close();
                    aggregator.close();
                } catch (Exception e) {
                    logger.error("Error flushing aggregation to coordinator", e);
                }
            }

            @Override
            public void setScorer(Scorable scorable) throws IOException {
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setWeight(Weight weight) {
        if (this.in != null) {
            this.in.setWeight(weight);
        }
    }
}
