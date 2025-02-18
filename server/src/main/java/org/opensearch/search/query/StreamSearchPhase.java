/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;
import org.opensearch.search.stream.collector.ArrowCollector;
import org.opensearch.search.stream.collector.ArrowFieldAdaptor;
import org.opensearch.search.stream.collector.ArrowStreamingCollector;
import org.opensearch.search.stream.collector.PushStreamingCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.search.stream.collector.ArrowFieldAdaptor.getArrowType;

/**
 * StreamSearchPhase is the search phase for streaming search.
 */
public class StreamSearchPhase extends QueryPhase {
    public static Logger logger = LogManager.getLogger(StreamSearchPhase.class);

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
     * DefaultStreamSearchPhaseSearcher
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

//            List<FieldAndFormat> fields = searchContext.fetchFieldsContext().fields();
//            searchContext.request().source().aggregations().getAggregatorFactories().stream().map(b -> b.)
            // map from OpenSearch field to Arrow Field type
            List<ArrowFieldAdaptor> arrowFieldAdaptors = new ArrayList<>();
//            fields.forEach(field -> {
//                System.out.println("field: " + field.field);
            String fieldName = null;
            GlobalOrdinalsStringTermsAggregator collector = null;
            try {
                collector = (GlobalOrdinalsStringTermsAggregator) QueryCollectorContext.createQueryCollector(collectors);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            fieldName = collector.fieldName;

//            String fieldName = ((ValuesSource.Bytes.WithOrdinals.FieldData) searchContext).getIndexFieldName();
            QueryShardContext shardContext = searchContext.getQueryShardContext();
            MappedFieldType fieldType = shardContext.fieldMapper(fieldName);
            ArrowType arrowType = getArrowType(fieldType.typeName());
            arrowFieldAdaptors.add(new ArrowFieldAdaptor(fieldType.name(), arrowType, fieldType.typeName()));

            if (streamManager == null) {
                throw new RuntimeException("StreamManager not setup");
            }
            final boolean[] isCancelled = { false };
            final Schema[] schema = { null };
            final Optional<VectorSchemaRoot>[] root = new Optional[] { Optional.empty() };
            final Optional<VectorSchemaRoot>[] collectionRoot = new Optional[] { Optional.empty() };
            Collector finalCollector = collector;

            StreamTicket ticket = streamManager.registerStream(new StreamProducer() {

                @Override
                public void close() {
                    isCancelled[0] = true;
                    if (root[0].isPresent()) {
                        root[0].get().close();
                    }
                    if (collectionRoot[0].isPresent()) {
                        collectionRoot[0].get().close();
                    }
                }

                @Override
                public BatchedJob createJob(BufferAllocator allocator) {
                    return new BatchedJob() {

                        @Override
                        public void run(VectorSchemaRoot root, StreamProducer.FlushSignal flushSignal) {
                            try {
                                Map<String, Field> arrowFields = new HashMap<>();
                                DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
                                Map<String, FieldVector> fieldVectors = new HashMap<>();

                                arrowFieldAdaptors.forEach(field -> {
                                    long dictId = field.getFieldName().hashCode();
                                    DictionaryEncoding encoding = new DictionaryEncoding(dictId, false, null);
                                    // Create dictionary vector
                                    FieldType dictFieldType = new FieldType(true, field.getArrowType(), null);
                                    VarCharVector dictionaryVector = new VarCharVector("dict_" + field.getFieldName(), dictFieldType, allocator);

                                    // Create Dictionary instance and register with provider
                                    Dictionary dictionary = new Dictionary(dictionaryVector, encoding);
                                    provider.put(dictionary);

                                    // Create encoded vector with dictionary encoding reference
                                    FieldType encodedType = new FieldType(true, new ArrowType.Int(64, false), null);
                                    Field arrowField = new Field(field.getFieldName(), encodedType, null);
                                    UInt8Vector vector = (UInt8Vector) arrowField.createVector(allocator);

                                    arrowFields.put(field.getFieldName(), arrowField);
                                    fieldVectors.put(field.getFieldName(), vector);
                                });
                                Schema schema = new Schema(new ArrayList<>(arrowFields.values()));
                                VectorSchemaRoot collectionRoot = new VectorSchemaRoot(
                                    schema,
                                    new ArrayList<>(fieldVectors.values()),
                                    0
                                );
                                final PushStreamingCollector arrowDocIdCollector = new PushStreamingCollector(finalCollector, provider, collectionRoot, root, allocator, arrowFieldAdaptors, 1_000_000, flushSignal, searchContext.shardTarget().getShardId());
//                                final ArrowStreamingCollector arrowDocIdCollector = new ArrowStreamingCollector(finalCollector, provider, collectionRoot, root, allocator, arrowFieldAdaptors, 1_000_000, flushSignal, searchContext.shardTarget().getShardId());
                                try {
                                    searcher.addQueryCancellation(() -> {
                                        if (isCancelled[0] == true) {
                                            throw new OpenSearchException("Stream for query results cancelled.");
                                        }
                                    });
                                    searcher.search(query, arrowDocIdCollector);
                                } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                                    // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of
                                    // collection. Postcollection
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
                            isCancelled[0] = true;
                        }

                        @Override
                        public boolean isCancelled() {
                            return searchContext.isCancelled() || isCancelled[0];
                        }
                    };
                }

                @Override
                public TimeValue getJobDeadline() {
                    return TimeValue.timeValueMinutes(5);
                }

                @Override
                public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                    Map<String, Field> arrowFields = new HashMap<>();
                    Field countField = new Field("count", FieldType.nullable(new ArrowType.Int(64, false)), null);
                    arrowFields.put("count", countField);
                    arrowFields.put("ord", new Field("ord", FieldType.nullable(new ArrowType.Utf8()), null));

                    schema[0] = new Schema(arrowFields.values());
                    root[0] = Optional.of(VectorSchemaRoot.create(schema[0], allocator));
                    return root[0].get();
                }

                @Override
                public int estimatedRowCount() {
                    return searcher.getIndexReader().numDocs();
                }

                @Override
                public String getAction() {
                    return searchContext.getTask().getAction();
                }
            }, searchContext.getTask().getParentTaskId());
            StreamSearchResult streamSearchResult = searchContext.streamSearchResult();
            streamSearchResult.flights(List.of(new OSTicket(ticket.toBytes())));
            return false;
        }
    }
}
