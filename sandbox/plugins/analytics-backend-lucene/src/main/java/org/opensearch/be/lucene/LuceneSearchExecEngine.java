/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.delegation.filter.FilterDelegationTarget;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

/**
 * Lucene-backed search execution engine and filter delegation target.
 * <p>
 * Implements {@link SearchExecEngine} for direct query execution and
 * {@link FilterDelegationTarget} for evaluating filter predicates on
 * behalf of another backend (e.g., DataFusion).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchExecEngine implements SearchExecEngine, FilterDelegationTarget {

    private static final Logger logger = LogManager.getLogger(LuceneSearchExecEngine.class);

    private final LuceneSearchContext context;

    public LuceneSearchExecEngine(LuceneSearchContext context) {
        this.context = context;
    }

    @Override
    public void prepare(ExecutionContext requestContext) {
        // TODO: extract Lucene Query from the resolved plan's filter predicates
    }

    @Override
    public EngineResultStream execute(ExecutionContext requestContext) throws IOException {
        LuceneEngineSearcher searcher = new LuceneEngineSearcher(
            new IndexSearcher(context.getReader()), context.getReader());
        searcher.search(context);
        // TODO: return a result stream wrapping Lucene's TopDocs/DocValues
        return null;
    }

    @Override
    public long[] delegateFilter(String targetBackend, int segmentOrd, int minDocId, int maxDocId) {
        logger.info("[LuceneSearchExecEngine] delegateFilter: backend={}, segment={}, docs=[{}, {})",
            targetBackend, segmentOrd, minDocId, maxDocId);

        try {
            context.ensureWeightPrepared();
            List<LeafReaderContext> leaves = context.getLeaves();

            if (segmentOrd >= leaves.size()) {
                logger.warn("Segment ordinal {} out of range (leaves={})", segmentOrd, leaves.size());
                return new long[0];
            }

            LeafReaderContext leaf = leaves.get(segmentOrd);
            int numDocs = maxDocId - minDocId;
            BitSet bitset = new BitSet(numDocs);

            Scorer scorer = context.getWeight().scorer(leaf);
            if (scorer != null) {
                DocIdSetIterator it = scorer.iterator();
                int doc = it.advance(minDocId);
                while (doc < maxDocId) {
                    bitset.set(doc - minDocId);
                    doc = it.nextDoc();
                }
            }

            logger.info("[LuceneSearchExecEngine] delegateFilter result: segment={}, matches={}",
                segmentOrd, bitset.cardinality());
            return bitset.toLongArray();
        } catch (IOException e) {
            logger.error("delegateFilter failed for segment {}", segmentOrd, e);
            return new long[0];
        }
    }

    @Override
    public long[] getSegmentMaxDocs() {
        try {
            context.ensureWeightPrepared();
            List<LeafReaderContext> leaves = context.getLeaves();
            long[] maxDocs = new long[leaves.size()];
            for (int i = 0; i < leaves.size(); i++) {
                maxDocs[i] = leaves.get(i).reader().maxDoc();
            }
            return maxDocs;
        } catch (IOException e) {
            logger.error("Failed to prepare weight for getSegmentMaxDocs", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        context.close();
    }
}
