/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.LongArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StreamingAggregator extends FilterCollector {

    private final Aggregator aggregator;
    private final SearchContext searchContext;
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final ShardId shardId;

    /**
     * Sole constructor.
     *
     * @param in
     */
    public StreamingAggregator(
        Aggregator in,
        SearchContext searchContext,
        VectorSchemaRoot root,
        int batchSize,
        StreamProducer.FlushSignal flushSignal,
        ShardId shardId
    ) {
        super(in);
        this.aggregator = in;
        this.searchContext = searchContext;
        this.root = root;
        this.batchSize = batchSize;
        this.flushSignal = flushSignal;
        this.shardId = shardId;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

        Map<String, FieldVector> vectors = new HashMap<>();
        vectors.put("ord", root.getVector("ord"));
        vectors.put("count", root.getVector("count"));
        final int[] currentRow = { 0 };
        String fieldName = ((GlobalOrdinalsStringTermsAggregator) aggregator).fieldName;
        SortedSetDocValues dv = context.reader().getSortedSetDocValues(fieldName);
        long maxOrd = dv.getValueCount();
        LongArray longArray = searchContext.bigArrays().newLongArray(maxOrd, true);
        return new LeafBucketCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {

            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                dv.advance(doc);
                for (int i = 0; i < dv.docValueCount(); i++) {
                    long ord = dv.nextOrd();
                    longArray.increment(ord, 1);
                }
            }

            private void flushBatch() throws IOException {
                int bucketCount = 0;
                VarCharVector termVector = (VarCharVector) vectors.get("ord");
                UInt8Vector countVector = (UInt8Vector) vectors.get("count");
                for (int i = 0; i < longArray.size(); i++) {
                    long cnt = longArray.get(i);
                    if (cnt > 0) {
                        BytesRef term = BytesRef.deepCopyOf(dv.lookupOrd(i));
                        termVector.setSafe(i, DocValueFormat.RAW.format(term).toString().getBytes(StandardCharsets.UTF_8));
                        countVector.setSafe(i, cnt);
                        bucketCount++;
                    }
                }
                aggregator.reset();
                // Reset for next batch
                root.setRowCount(bucketCount);
                // System.out.println("## Flushing batch of size: " + bucketCount);
                flushSignal.awaitConsumption(TimeValue.timeValueMillis(100000));
                currentRow[0] = 0;
            }

            @Override
            public void finish() throws IOException {
                flushBatch();
                longArray.close();
            }
        };
    }
}
