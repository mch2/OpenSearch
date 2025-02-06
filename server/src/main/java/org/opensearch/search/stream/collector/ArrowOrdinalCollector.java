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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow collector for OpenSearch fields values
 */
@ExperimentalApi
public class ArrowOrdinalCollector extends FilterCollector {

    private final VectorSchemaRoot collectionRoot;
    private final DictionaryProvider provider;
    private final BufferAllocator allocator;
    List<ArrowFieldAdaptor> fields;
    private final VectorSchemaRoot bucketRoot;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;
    public static Logger logger = LogManager.getLogger(ArrowOrdinalCollector.class);
    // Pre-allocate reusable buffers
//    private final int[] docIds;
//    private final long[] ords;
//    private final long[] sortedOrds;
//    private final int[] positions;
//    private final int[] lengths;
    private byte[] terms;

    public ArrowOrdinalCollector(
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
        this.batchSize = batchSize;
        this.flushSignal = flushSignal;
        this.shardId = shardId;
        // Pre-allocate arrays
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final int batchSize = (int) ((ArrowFieldAdaptor.SortedDocValuesType) fields.get(0).getDocValues(context.reader())).getSortedDocValues().getValueCount();
        Map<String, ArrowFieldAdaptor.DocValuesType> docValueIterators = new HashMap<>();
        String term = fields.stream().findFirst().get().getFieldName();
        fields.forEach(field -> {
            try {
                ArrowFieldAdaptor.DocValuesType dv = field.getDocValues(context.reader());
                docValueIterators.put(field.fieldName, dv);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        final int[] docIds = new int[batchSize];
        final long[] ords = new long[batchSize];
        final long[] sortedOrds = new long[batchSize];
        final int[] positions = new int[batchSize];
        final int[] lengths = new int[batchSize];
        terms = new byte[batchSize];




        final int[] currentRow = {0};
        final int[] totalDocs = {0};
        return new LeafCollector() {

            @Override
            public void collect(int docId) throws IOException {

                docIds[currentRow[0]] = docId;

                currentRow[0]++;
//                if (currentRow[0] == batchSize) {
//                    flushDocs();
//                }
            }

            private void flushDocs() throws IOException {
                logger.info("Starting flush of {} docs", currentRow[0]);

                // read from the lucene field values
                for (Map.Entry<String, ArrowFieldAdaptor.DocValuesType> entry : docValueIterators.entrySet()) {

                    String field = entry.getKey();

                    ArrowFieldAdaptor.DocValuesType dv = entry.getValue();
                    boolean numeric = false;
                    SortedNumericDocValues numericDocValues = null;
                    SortedSetDocValues sortedDocValues = null;
                    if (dv instanceof ArrowFieldAdaptor.NumericDocValuesType) {
                        numericDocValues = ((ArrowFieldAdaptor.NumericDocValuesType) dv).getNumericDocValues();
                        numeric = true;
                    } else if (dv instanceof ArrowFieldAdaptor.SortedDocValuesType) {
                        sortedDocValues = ((ArrowFieldAdaptor.SortedDocValuesType) dv).getSortedDocValues();
                    }

                    FieldVector vector = collectionRoot.getVector(field);
//                    Map<Long, Integer> ordToIndex = new HashMap<>();
                    Dictionary lookup = provider.lookup(field.hashCode());
                    VarCharVector dictVector = (VarCharVector) lookup.getVector();
                    int dictRow = 0;
                    int ordRow = 0;
//                    long[] ordToIndex = new long[batchSize];  // or estimate max size needed
//                    Arrays.fill(ordToIndex, -1);  // use -1 as sentinel
                    for (int i = 0; i < currentRow[0]; i++) {
                        if (numeric) {
                            if (numericDocValues.advanceExact(docIds[i])) {
                                long value = numericDocValues.nextValue();
                                ((BigIntVector) vector).setSafe(i, value);
                            }
                        } else {
                            if (sortedDocValues.advanceExact(docIds[i])) {
                                ords[i] = sortedDocValues.nextOrd();
//                                long ord = sortedDocValues.nextOrd();
//                                if (ord >= ordToIndex.length) {
//                                    ordToIndex = Arrays.copyOf(ordToIndex, Math.max(ordToIndex.length * 2, (int) ord + 1));
//                                }
//                                if (ordToIndex[(int) ord] == -1) {
//                                    BytesRef bytesRef = sortedDocValues.lookupOrd(ord);
//                                    dictVector.setSafe(dictRow, bytesRef.bytes, bytesRef.offset, bytesRef.length);
//                                    ordToIndex[(int) ord] = dictRow;
//                                    dictRow++;
//                                }
//                                ((UInt8Vector) vector).setSafe(ordRow, (int) ordToIndex[(int) ord]);
//                                ordRow++;
                            } else {
                                ords[i] = -1;
                            }
                        }
                    }
                    if (!numeric) {
                        int maxSize = 0;
                        System.arraycopy(ords, 0, sortedOrds, 0, currentRow[0]);
                        Arrays.sort(sortedOrds, 0, currentRow[0]);
                        for (int i = 0; i < currentRow[0]; i++) {
                            int ordIdx = Arrays.binarySearch(sortedOrds, 0, currentRow[0], ords[i]);
                            ords[i] = ordIdx;
                        }
                        int pos = 0;
                        long prevOrd = -2;
                        for (int i = 0; i < currentRow[0]; i++) {
                            long curOrd = sortedOrds[i];
                            if (curOrd < 0) {
                                positions[i] = -1;
                                lengths[i] = -1;
                            } else {
                                if (curOrd != prevOrd) {
                                    BytesRef val = sortedDocValues.lookupOrd(curOrd);
                                    positions[i] = pos;
                                    lengths[i] = val.length;
                                    maxSize = Math.max(maxSize, val.length);
                                    terms = ArrayUtil.grow(terms, pos + val.length);
                                    System.arraycopy(val.bytes, val.offset, terms, pos, val.length);
                                    pos += val.length;
                                    prevOrd = curOrd;
                                } else {
                                    positions[i] = positions[i - 1];
                                    lengths[i] = lengths[i - 1];
                                }
                            }
                        }
                        int finalMaxSize = maxSize;
//                        fields.forEach(f -> {
//                            BaseVariableWidthVector v = (BaseVariableWidthVector) vectorCache.get(f.fieldName);
//                            v.reset();
//                            v.setInitialCapacity(currentRow[0], finalMaxSize);
//                            v.allocateNew();
//                        });

                        int[] ordToIndex = new int[batchSize];  // or estimate max size needed
                        Arrays.fill(ordToIndex, -1);  // use -1 as sentinel
//                        VarCharVector dictVector = (VarCharVector) collectionRoot.getVector("dict_" + field);
                        for (int i = 0; i < currentRow[0]; i++) {
                            int ordIdx = (int) ords[i];
                            int curOrd = (int) sortedOrds[ordIdx];
//                            logger.info("curOrd: {} all {}", curOrd, ordToIndex);
                            if (curOrd >= ordToIndex.length) {
                                ordToIndex = Arrays.copyOf(ordToIndex, Math.max(ordToIndex.length * 2, (int) curOrd + 1));
                            }
                            if (curOrd >= 0 && ordToIndex[curOrd] == -1) {
//                                logger.info("Writing into dictVector {} for ordIdx {}", dictIndex, ordIdx);
                                String s = new String(terms, positions[ordIdx], lengths[ordIdx]);
//                                logger.info("Writing {} into dictionary vector at position {}", s, i);
                                dictVector.setSafe(dictRow, terms, positions[ordIdx], lengths[ordIdx]);
                                ordToIndex[curOrd] = dictRow;
//                                ordToIndex.put(curOrd, dictRow);
                                dictRow++;
                            }
                            if (curOrd >= 0) {
                                int index = ordToIndex[curOrd];
                                ((UInt8Vector) vector).setSafe(ordRow, index);
                                ordRow++;
                            }
                        }
                        vector.setValueCount(ordRow);
//                        for (long dictId : provider.getDictionaryIds()) {
//                        logger.info("Setting {} dict values", ordToIndex.size());
                        dictVector.setValueCount(dictRow); // Set the actual number of values
//                        }
                    }
                }

                // DATAFUSION HANDOFF

                collectionRoot.setRowCount(currentRow[0]);
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try {
//                    logger.info("passing batch of {} docs to DF", currentRow[0]);
//                    logger.info(provider.lookup(provider.getDictionaryIds().stream().findFirst().get()).getVector().)
                        DataFrame dataFrame = DataFusion.from_vsr(allocator, collectionRoot, provider, term).get();
                        RecordBatchStream recordBatchStream = dataFrame.getStream(allocator).get();
                        VectorSchemaRoot root = recordBatchStream.getVectorSchemaRoot();
                        VarCharVector ordVector = (VarCharVector) bucketRoot.getVector("ord");
                        BigIntVector countVector = (BigIntVector) bucketRoot.getVector("count");
//                    int row = 0;
                        while (recordBatchStream.loadNextBatch().join()) {
                            int batchNumRows = root.getRowCount();
                            FieldVector encodedVector = root.getVector("ord");
                            FieldVector cv = root.getVector("count");
                            long dictId = encodedVector.getField().getDictionary().getId();

                            try (VarCharVector ordinals = (VarCharVector) DictionaryEncoder.decode(encodedVector, recordBatchStream.lookup(dictId))) {
                                logger.info("Value count {} {}", ordinals.getValueCount(), batchNumRows);
                                ordVector.setInitialCapacity(ordinals.getValueCapacity());
                                // Copy values one by one to ensure proper transfer
                                for (int i = 0; i < ordinals.getValueCount(); i++) {
                                    if (!ordinals.isNull(i)) {
                                        byte[] value = ordinals.get(i);
                                        // Get the actual length of this string
                                        int length = ordinals.getValueLength(i);
                                        // Copy exactly length bytes from the value array
                                        ordVector.setSafe(i, value, 0, length);
                                    }
                                }
                            }
                            // For count vector
                            for (int i = 0; i < cv.getValueCount(); i++) {
                                countVector.setSafe(i, ((BigIntVector) cv).get(i));
                            }
                            ordVector.setValueCount(batchNumRows);
                            countVector.setValueCount(batchNumRows);
//                        row[0] += batchNumRows;

//                        logger.info("KEYS {}", ordVector);
//                        logger.info("VALUES {}", countVector);
//                        logger.info("ROOT {}", bucketRoot);
                            bucketRoot.setRowCount(batchNumRows);
                            flushSignal.awaitConsumption(TimeValue.timeValueMillis(1000 * 120));
                        }
                        recordBatchStream.close(); // clean up DF pointers
                        collectionRoot.clear();
                        dataFrame.close();
                    } catch (Exception e) {
                        logger.error("eh", e);
                        throw new RuntimeException(e);
                    }
//                logger.info("Flushed {} docs", currentRow[0]);
                    totalDocs[0] += currentRow[0];
//                logger.info("Flushed {} docs total", totalDocs[0]);

//                flushSignal.awaitConsumption(1000 * 120);
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
