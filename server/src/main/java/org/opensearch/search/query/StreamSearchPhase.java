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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;
import org.opensearch.search.stream.collector.ArrowCollector;
import org.opensearch.search.stream.collector.ArrowDocIdCollector;
import org.opensearch.search.stream.collector.ArrowFieldAdaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.stream.collector.ArrowFieldAdaptor.getArrowType;

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

        @Override
        public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
            return new AggregationProcessor() {
                @Override
                public void preProcess(SearchContext context) {

                }

                @Override
                public void postProcess(SearchContext context) {

                }
            };
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

            List<FieldAndFormat> fields = searchContext.fetchFieldsContext().fields();

            // map from OpenSearch field to Arrow Field type
            List<ArrowFieldAdaptor> arrowFieldAdaptors = new ArrayList<>();
            fields.forEach(field -> {
                QueryShardContext shardContext = searchContext.getQueryShardContext();
                MappedFieldType fieldType = shardContext.fieldMapper(field.field);
                ArrowType arrowType = getArrowType(fieldType.typeName());
                arrowFieldAdaptors.add(new ArrowFieldAdaptor(field.field, arrowType, fieldType.typeName()));
            });

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
                                Collector collector = QueryCollectorContext.createQueryCollector(collectors);
                                final ArrowCollector arrowDocIdCollector = new ArrowCollector(collector, arrowFieldAdaptors, root,  2, flushSignal, searchContext.shardTarget().getShardId());
                                try {
                                    searcher.search(query, arrowDocIdCollector);
                                } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                                    // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of collection. Postcollection
                                    // still needs to be processed for Aggregations when early termination takes place.
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
                                if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
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

                    Field docIdField = new Field("docId", FieldType.notNullable(new ArrowType.Int(32, true)), null);
                    arrowFields.put("docId", docIdField);
                    Field scoreField = new Field(
                        "score",
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                        null
                    );
                    arrowFields.put("score", scoreField);

                    arrowFieldAdaptors.forEach(field -> {
                        Field arrowField = new Field(field.getFieldName(), FieldType.nullable(field.getArrowType()), null);
                        arrowFields.put(field.getFieldName(), arrowField);
                    });
                    arrowFields.put("shardId", new Field("shardId", FieldType.notNullable(new ArrowType.Utf8()), null));
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
