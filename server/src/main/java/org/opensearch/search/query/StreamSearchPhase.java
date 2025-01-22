/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.support.StreamingAggregator;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Produce stream from a shard search
 */
public class StreamSearchPhase extends QueryPhase {

    private static final Logger LOGGER = LogManager.getLogger(StreamSearchPhase.class);
    public static final QueryPhaseSearcher DEFAULT_QUERY_PHASE_SEARCHER = new DefaultStreamSearchPhaseSearcher();

    public StreamSearchPhase() {
        super(DEFAULT_QUERY_PHASE_SEARCHER);
    }

    @Override
    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }
        final AggregationProcessor aggregationProcessor = this.getQueryPhaseSearcher().aggregationProcessor(searchContext);
        aggregationProcessor.preProcess(searchContext);
        executeInternal(searchContext, this.getQueryPhaseSearcher());

        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(
                searchContext.getProfilers(),
                searchContext.request()
            );
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    /**
     * Default implementation of {@link QueryPhaseSearcher}.
     */
    public static class DefaultStreamSearchPhaseSearcher extends DefaultQueryPhaseSearcher {

        @Override
        public boolean searchWith(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }

        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasTimeout);
        }

        private boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean timeoutSet
        ) {

            QuerySearchResult queryResult = searchContext.queryResult();
            StreamManager streamManager = searchContext.streamManager();
            if (streamManager == null) {
                throw new RuntimeException("StreamManager not setup");
            }
            StreamTicket ticket = streamManager.registerStream(new StreamProducer() {
                @Override
                public BatchedJob createJob(BufferAllocator allocator) {
                    return new BatchedJob() {
                        @Override
                        public void run(VectorSchemaRoot root, StreamProducer.FlushSignal flushSignal) {
                            try {
                                final StreamingAggregator arrowDocIdCollector = new StreamingAggregator(
                                    (Aggregator) QueryCollectorContext.createQueryCollector(collectors),
                                    searchContext,
                                    root,
                                    1_000_000,
                                    flushSignal,
                                    searchContext.shardTarget().getShardId()
                                );
                                try {
                                    searcher.search(query, arrowDocIdCollector);
                                } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                                    searchContext.bucketCollectorProcessor().processPostCollection(arrowDocIdCollector);
                                    queryResult.terminatedEarly(true);
                                }
                                if (searchContext.isSearchTimedOut()) {
                                    assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                                    if (searchContext.request().allowPartialSearchResults() == false) {
                                        throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
                                    }
                                    queryResult.searchTimedOut(true);
                                }
                                if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER
                                    && queryResult.terminatedEarly() == null) {
                                    queryResult.terminatedEarly(false);
                                }

                                for (QueryCollectorContext ctx : collectors) {
                                    ctx.postProcess(queryResult);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onCancel() {

                        }
                    };
                }

                @Override
                public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                    Map<String, Field> arrowFields = new HashMap<>();
                    Field countField = new Field("count", FieldType.nullable(new ArrowType.Int(64, false)), null);
                    arrowFields.put("count", countField);
                    arrowFields.put("ord", new Field("ord", FieldType.nullable(new ArrowType.Utf8()), null));
                    Schema schema = new Schema(arrowFields.values());
                    return VectorSchemaRoot.create(schema, allocator);
                }

                @Override
                public int estimatedRowCount() {
                    return searcher.getIndexReader().numDocs();
                }
            });
            StreamSearchResult streamSearchResult = searchContext.streamSearchResult();
            streamSearchResult.flights(List.of(new OSTicket(ticket.getTicketID(), ticket.getNodeID())));
            return false;
        }
    }
}
