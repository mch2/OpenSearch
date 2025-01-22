/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream.collector;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow collector for OpenSearch fields values
 */
@ExperimentalApi
public class ArrowCollector extends FilterCollector {

    List<ArrowFieldAdaptor> fields;
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;

    public ArrowCollector(
        Collector in,
        List<ArrowFieldAdaptor> fields,
        VectorSchemaRoot root,
        int batchSize,
        StreamProducer.FlushSignal flushSignal,
        ShardId shardId
    ) {
        super(in);
        this.fields = fields;
        this.root = root;
        this.batchSize = batchSize;
        this.flushSignal = flushSignal;
        this.shardId = shardId;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final int docBase = context.docBase;
        Map<String, ArrowFieldAdaptor.DocValuesType> docValueIterators = new HashMap<>();
        Map<String, FieldVector> vectors = new HashMap<>();
        // TODO bowen the vector we get from root may not work with concurrent segment search?
        // looks fine if the segment search is executed in sequential
        vectors.put("docId", root.getVector("docId"));
        vectors.put("score", root.getVector("score"));
        vectors.put("shardId", root.getVector("shardId"));
        fields.forEach(field -> {
            try {
                ArrowFieldAdaptor.DocValuesType dv = field.getDocValues(context.reader());
                docValueIterators.put(field.fieldName, dv);
                vectors.put(field.fieldName, root.getVector(field.fieldName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        final int[] currentRow = { 0 };
        return new LeafCollector() {
            private final int[] docIds = new int[batchSize];
            private final float[] scores = new float[batchSize];

            private Scorable scorer;

            @Override
            public void collect(int docId) throws IOException {

                docIds[currentRow[0]] = docId;
                scores[currentRow[0]] = scorer.score();

                currentRow[0]++;
                if (currentRow[0] == batchSize) {
                    flushDocs();
                }
            }

            private void flushDocs() throws IOException {
                for (int i = 0; i < currentRow[0]; i++) {
                    FieldVector docIDVector = vectors.get("docId");
                    ((IntVector) docIDVector).setSafe(i, docIds[i] + docBase);

                    FieldVector scoreVector = vectors.get("score");
                    ((Float4Vector) scoreVector).setSafe(i, scores[i]);

                    VarCharVector shardIdVector = (VarCharVector) vectors.get("shardId");
                    shardIdVector.setSafe(i, shardId.toString().getBytes());
                }

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

                    FieldVector vector = vectors.get(field);
                    for (int i = 0; i < currentRow[0]; i++) {
                        if (numeric) {
                            if (numericDocValues.advanceExact(docIds[i])) {
                                long value = numericDocValues.nextValue();
                                ((BigIntVector) vector).setSafe(i, value);
                            }
                        } else {
                            if (sortedDocValues.advanceExact(docIds[i])) {
                                long ord = sortedDocValues.nextOrd();
                                BytesRef keyword = sortedDocValues.lookupOrd(ord);
                                ((VarCharVector) vector).setSafe(i, keyword.utf8ToString().getBytes());
                            }
                        }
                    }
                }
                root.setRowCount(currentRow[0]);
                flushSignal.awaitConsumption(1000);
                currentRow[0] = 0;
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    flushDocs();
                }
            }

            @Override
            public void setScorer(Scorable scorable) throws IOException {
                // innerLeafCollector.setScorer(scorable);
                this.scorer = scorable;
            }
        }

        ;
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
