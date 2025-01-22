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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.action.search.SearchPhaseController;
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
import org.opensearch.search.aggregations.metrics.InternalCardinality;
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
    public static Logger logger = LogManager.getLogger(StreamingAggregator.class);

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        Map<String, FieldVector> vectors = new HashMap<>();
        vectors.put("ord", root.getVector("ord"));
        vectors.put("count", root.getVector("count"));
        final int[] currentRow = {0};
        final LeafBucketCollector leaf = aggregator.getLeafCollector(context);
        return new LeafBucketCollector() {


            @Override
            public void setScorer(Scorable scorer) throws IOException {

            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                leaf.collect(doc);
                currentRow[0]++;
                if (currentRow[0] == batchSize) {
                    flushBatch();
                    flushSignal.awaitConsumption(10000000);
                }
            }

            @Override
            public void finish() throws IOException {
                if (currentRow[0] > 0) {
                    flushBatch();
                    flushSignal.awaitConsumption(10000000);
                    logger.info("Flushed last batch for segment {}", context.toString());
                }
            }

            private void flushBatch() throws IOException {
                InternalAggregation agg = aggregator.buildAggregations(new long[]{0})[0];

                    int bucketNum = 0;
                FieldVector termVector = vectors.get("ord");
                FieldVector countVector = vectors.get("count");
                if (agg instanceof InternalMappedTerms) {
                    InternalMappedTerms<?,?> terms = (InternalMappedTerms<?,?>) agg;

                    List<? extends InternalTerms.Bucket> buckets = terms.getBuckets();
                    for (InternalTerms.Bucket bucket : buckets) {
                        // Get key/value info
                        String key = bucket.getKeyAsString();

                        long docCount = bucket.getDocCount();
//                        Aggregations aggregations = bucket.getAggregations();
//                        for (Aggregation aggregation : aggregations) {
//                        // TODO: subs
//                        }

                        // Write to vector storage
                        // e.g., for term and count vectors:
//                        VarCharVector keyVector = (VarCharVector) vectors.get("key");
//                        keyVector.setSafe(i, key.getBytes());
//                        logger.info("KEY {} VAL {}", ke)

                        ((VarCharVector) termVector).setSafe(bucketNum, key.getBytes());
                        ((UInt8Vector) countVector).setSafe(bucketNum, docCount);
//                        logger.info("Writing {} for {}", docCount, key);

                        // Add the values...
                        bucketNum++;
                    }


                    // Also access high-level statistics
//                    long otherDocCount = terms.getSumOfOtherDocCounts();
//                    long docCountError = terms.getDocCountError();
                } else if (agg instanceof InternalCardinality) {
                    InternalCardinality ic = (InternalCardinality) agg;
                    String key = ic.getName();
                    long count = ic.getValue();
                    ((VarCharVector) termVector).setSafe(bucketNum, key.getBytes());
                    ((UInt8Vector) countVector).setSafe(bucketNum, count);
                    logger.info("Writing cardinality {} for {}", count, key);
                    bucketNum++;
                }

                // Reset for next batch
                logger.info("flush after {} docs with {} buckets", currentRow[0], bucketNum);
                root.setRowCount(bucketNum);
                logger.info("Awaiting consumption at data node");
                flushSignal.awaitConsumption(10000000);
                logger.info("Consumed batch at data node");
                aggregator.reset();
                currentRow[0] = 0;
            }
        };
    }
}
