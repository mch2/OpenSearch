/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.terms.InternalMappedTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
        final int[] currentRow = {0};
        return new LeafBucketCollector() {


            @Override
            public void setScorer(Scorable scorer) throws IOException {

            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                final LeafBucketCollector leaf = aggregator.getLeafCollector(context);
                leaf.collect(doc);
                currentRow[0]++;
                if (currentRow[0] == batchSize) {
                    flushBatch();
                }

                // hit batch size

                // flush
            }

            private void flushBatch() throws IOException {
                InternalAggregation agg = aggregator.buildAggregations(new long[]{0})[0];
                if (agg instanceof InternalMappedTerms) {
                    InternalMappedTerms<?,?> terms = (InternalMappedTerms<?,?>) agg;

                    List<? extends InternalTerms.Bucket> buckets = terms.getBuckets();
                    for (InternalTerms.Bucket bucket : buckets) {
                        // Get key/value info
                        String key = bucket.getKeyAsString();
                        long docCount = bucket.getDocCount();

                        Aggregations aggregations = bucket.getAggregations();
                        for (Aggregation aggregation : aggregations) {
                        // TODO: subs
                        }

                        // Write to vector storage
                        // e.g., for term and count vectors:
//                        VarCharVector keyVector = (VarCharVector) vectors.get("key");
//                        keyVector.setSafe(i, key.getBytes());
                        FieldVector termVector = vectors.get("ord");
                        FieldVector countVector = vectors.get("count");
                        ((VarCharVector) termVector).setSafe(0, key.getBytes());
                        ((Float4Vector) countVector).setSafe(0, docCount);

                        // Add the values...
                    }

                    aggregator.reset();

                    // Also access high-level statistics
//                    long otherDocCount = terms.getSumOfOtherDocCounts();
//                    long docCountError = terms.getDocCountError();
                }

                // Reset for next batch
                currentRow[0] = 0;
                root.setRowCount(currentRow[0]);
                flushSignal.awaitConsumption(1000);
            }
        };
    }
}
