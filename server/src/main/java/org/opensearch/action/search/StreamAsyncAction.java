/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.DataFrame;
import org.opensearch.datafusion.DataFrameStreamProducer;
import org.opensearch.datafusion.DataFusion;
import org.opensearch.datafusion.RecordBatchStream;
import org.opensearch.datafusion.SessionContext;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardStreamQueryResult;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.transport.Transport;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Async transport action for query then fetch
 *
 * @opensearch.internal
 */
class StreamAsyncAction extends SearchQueryThenFetchAsyncAction {

    public static Logger logger = LogManager.getLogger(StreamAsyncAction.class);
    private final SearchPhaseController searchPhaseController;

    public StreamAsyncAction(Logger logger, SearchTransportService searchTransportService, BiFunction<String, String, Transport.Connection> nodeIdToConnection, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, Map<String, Set<String>> indexRoutings, SearchPhaseController searchPhaseController, Executor executor, QueryPhaseResultConsumer resultConsumer, SearchRequest request, ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts, TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState, SearchTask task, SearchResponse.Clusters clusters, SearchRequestContext searchRequestContext, Tracer tracer) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings, searchPhaseController, executor, resultConsumer, request, listener, shardsIts, timeProvider, clusterState, task, clusters, searchRequestContext, tracer);
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new StreamSearchReducePhase("stream_reduce", context);
    }

    class StreamSearchReducePhase extends SearchPhase {
        private SearchPhaseContext context;

        protected StreamSearchReducePhase(String name, SearchPhaseContext context) {
            super(name);
            this.context = context;
        }

        @Override
        public void run() {
            context.execute(new StreamReduceAction(context, this));
        }
    };

    class StreamReduceAction extends AbstractRunnable {
        private SearchPhaseContext context;
        private SearchPhase phase;

        StreamReduceAction(SearchPhaseContext context, SearchPhase phase) {
            this.context = context;
        }

        @Override
        protected void doRun() throws Exception {
            try {
                // fetch all the tickets (one byte[] per shard) and hand that off to Datafusion.Query
                // this creates a single stream that we'll register with the streammanager on this coordinator.
                List<SearchPhaseResult> results = StreamAsyncAction.this.results.getAtomicArray().asList();
                List<byte[]> tickets = results.stream().flatMap(r -> ((StreamSearchResult) r).getFlightTickets().stream())
                    .map(OSTicket::getBytes)
                    .collect(Collectors.toList());

                // This is additional metadata for the fetch phase that will be conducted on the coordinator
                // StreamTargetResponse is a wrapper for an individual shard that contains the contextId and ShardTarget that served the original
                // query phase so we can fetch from it.
                List<StreamTargetResponse> targets = StreamAsyncAction.this.results.getAtomicArray().asList()
                    .stream()
                    .map(r -> new StreamTargetResponse(r.queryResult(), r.getSearchShardTarget()))
                    .collect(Collectors.toList());

                StreamManager streamManager = searchPhaseController.getStreamManager();
                StreamTicket streamTicket = streamManager.registerStream(DataFrameStreamProducer.query(tickets));
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(SearchHits.empty(), null, null, null, false, false, 1, Collections.emptyList(), List.of(new OSTicket(streamTicket.getTicketID(), streamTicket.getNodeID())), targets);
                context.sendSearchResponse(internalSearchResponse, StreamAsyncAction.this.results.getAtomicArray());
            } catch (Exception e) {
                logger.error("broken", e);
                throw e;
            }
        }

        @Override
        public void onFailure(Exception e) {
            context.onPhaseFailure(phase, "", e);
        }
    }
}
