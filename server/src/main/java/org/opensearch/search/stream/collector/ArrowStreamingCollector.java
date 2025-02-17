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
import org.opensearch.datafusion.DataFusion;
import org.opensearch.datafusion.RecordBatchStream;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

/**
 * Arrow collector for OpenSearch fields values
 */
@ExperimentalApi
public class ArrowStreamingCollector extends FilterCollector {

    private final VectorSchemaRoot collectionRoot;
    private final DictionaryProvider provider;
    private final BufferAllocator allocator;
    List<ArrowFieldAdaptor> fields;
    private final VectorSchemaRoot bucketRoot;
    private final StreamProducer.FlushSignal flushSignal;
    public static Logger logger = LogManager.getLogger(ArrowStreamingCollector.class);
    // Pre-allocate reusable buffers
    private byte[] terms;
    private int batchSize;

    public ArrowStreamingCollector(
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
        this.provider = provider;
        this.allocator = allocator;
        this.fields = fields;
        this.bucketRoot = root;
        this.collectionRoot = collectionRoot;
        this.flushSignal = flushSignal;
        this.batchSize = batchSize;
        // Pre-allocate arrays
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        ArrowFieldAdaptor arrowFieldAdaptor = fields.get(0);
        String term = arrowFieldAdaptor.fieldName;
        SortedSetDocValues dv = ((ArrowFieldAdaptor.SortedDocValuesType) arrowFieldAdaptor.getDocValues(context.reader())).getSortedDocValues();
        final int maxOrd = (int) dv.getValueCount();

        // vector to hold ordinals
        FieldVector vector = collectionRoot.getVector(arrowFieldAdaptor.fieldName);
        vector.setInitialCapacity(maxOrd);

        final int[] currentRow = {0};
        return new LeafCollector() {

            @Override
            public void collect(int docId) throws IOException {
                if (currentRow[0] >= batchSize) {
                    flushDocs();
                }
                // dump all the ords into an arrow vector, df will aggregate on these and then decode
                dv.advance(docId);
                for (int i = 0; i < dv.docValueCount(); i++) {
                    long ord = dv.nextOrd();
                    ((UInt8Vector) vector).setSafe(currentRow[0], ord);
                }
                currentRow[0] += dv.docValueCount();
            }

            private void flushDocs() throws IOException {

                // set ord value count
                vector.setValueCount(currentRow[0]);
                collectionRoot.setRowCount(currentRow[0]);
                logger.info("Starting flush of {} docs", currentRow[0]);

                // DATAFUSION HANDOFF
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try {
                        DataFrame dataFrame = DataFusion.from_vsr(allocator, collectionRoot, provider, term).get();
                        RecordBatchStream recordBatchStream = dataFrame.getStream(allocator).get();
                        VectorSchemaRoot root = recordBatchStream.getVectorSchemaRoot();
                        VarCharVector ordVector = (VarCharVector) bucketRoot.getVector("ord");
                        BigIntVector countVector = (BigIntVector) bucketRoot.getVector("count");
                        int row = 0;
                        while (recordBatchStream.loadNextBatch().join()) {
                            UInt8Vector dfVector = (UInt8Vector) root.getVector("ord");
                            FieldVector cv = root.getVector("count");
                            logger.info("DF VECTOR {}", dfVector);
                            logger.info("COUNT VECTOR {}", cv);
                            // Create transfer pair for the count vector
//                            TransferPair countTransfer = cv.makeTransferPair(countVector);

                            // Transfer the counts
//                            countTransfer.transfer();
                            // for each row
                            for (int i = 0; i < dfVector.getValueCount(); i++) {
                                // look up ord value
                                BytesRef bytesRef = dv.lookupOrd(dfVector.get(i));
                                ordVector.setSafe(row, bytesRef.bytes, 0, bytesRef.length);
                                countVector.setSafe(row, ((BigIntVector) cv).get(i));
                                row++;
                            }
                            ordVector.setValueCount(row);
                            countVector.setValueCount(row);
                            bucketRoot.setRowCount(row);
                            flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000 * 120));
                            row = 0;
                            ordVector.clear();
                            countVector.clear();;
                            bucketRoot.setRowCount(0);
                        }
//                       flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000 * 120));
                        recordBatchStream.close(); // clean up DF pointers
                        collectionRoot.clear();
                        dataFrame.close();
                    } catch (Exception e) {
                        logger.error("eh", e);
                        throw new RuntimeException(e);
                    }
                    currentRow[0] = 0;
                    return null;
                });
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    flushDocs();
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
