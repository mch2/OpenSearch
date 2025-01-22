/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.arrow.StreamIterator;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.DataFrameStreamProducer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action for initiating a join, gets a single stream back.
 */
public class TransportStreamedJoinAction extends HandledTransportAction<JoinRequest, JoinResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportSearchAction transportSearchAction;
    private final StreamManager streamManager;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final IndicesService indicesService;

    @Inject
    public TransportStreamedJoinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportSearchAction transportSearchAction,
        StreamManager streamManager,
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        IndicesService indicesService
    ) {
        super(StreamedJoinAction.NAME, transportService, actionFilters, JoinRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportSearchAction = transportSearchAction;
        this.streamManager = streamManager;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.indicesService = indicesService;
    }

    /**
     * Do the thing
     */
    @Override
    protected void doExecute(Task task, JoinRequest request, ActionListener<JoinResponse> listener) {
        GroupedActionListener<SearchResponse> groupedListener = new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<SearchResponse> collection) {
                List<SearchResponse> responses = new ArrayList<>(collection);
                StreamTicket streamTicket = streamManager.registerStream(
                    DataFrameStreamProducer.join(tickets(responses.get(0)), tickets(responses.get(1)), request.getJoinField())
                );
                if (request.isGetHits()) {
                    getHits(task, request, responses, streamTicket, listener);
                } else {
                    listener.onResponse(new JoinResponse(new OSTicket(streamTicket.getTicketID(), streamTicket.getNodeID())));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException(e));
            }
        }, 2);
        searchIndex(task, request.getLeftIndex(), groupedListener);
        searchIndex(task, request.getRightIndex(), groupedListener);
    }

    private void getHits(
        Task task,
        JoinRequest request,
        List<SearchResponse> responses,
        StreamTicket ticket,
        ActionListener<JoinResponse> listener
    ) {
        String leftIndex = request.getLeftIndex().indices()[0];
        String rightIndex = request.getRightIndex().indices()[0];

        System.out.println("left index name: " + leftIndex);
        System.out.println("right index name: " + rightIndex);

        Map<String, StreamTargetResponse> targets = new HashMap<>();
        for (SearchResponse respons : responses) {
            List<StreamTargetResponse> shardResults = respons.getInternalResponse().getShardResults();
            for (StreamTargetResponse shardResult : shardResults) {
                targets.putIfAbsent(shardResult.getSearchShardTarget().getShardId().toString(), shardResult);
            }
        }

        targets = MapBuilder.newMapBuilder(targets).immutableMap();

        StreamIterator streamIterator = streamManager.getStreamIterator(ticket);
        // read a stream of query results.

        // just collect everything
        List<Hit> hits = new ArrayList<>();
        while (streamIterator.next()) {
            VectorSchemaRoot root = streamIterator.getRoot();
            int rowCount = root.getRowCount();
            // Iterate through rows
            for (int row = 0; row < rowCount; row++) {
                FieldVector id = root.getVector("docId");
                FieldVector sid = root.getVector("shardId");
                FieldVector right_docId = root.getVector("right_docId");
                FieldVector right_shardId = root.getVector("right_shardId");
                FieldVector joinField = root.getVector(request.getJoinField());
                ShardId shardId = ShardId.fromString((String) getValue(sid, row));
                ShardId right_sid = ShardId.fromString((String) getValue(right_shardId, row));

                int docId = (int) getValue(id, row);
                int right_id = (int) getValue(right_docId, row);
                hits.add(new Hit(docId, shardId, right_id, right_sid, getValue(joinField, row)));
            }
        }

        // for each hit split by index
        Map<String, Map<ShardId, List<Hit>>> hitsByIndex = organizeHitsByIndexAndShard(hits);

        GroupedActionListener<HitsPerIndex> groupedListener = new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<HitsPerIndex> maps) {

                Map<Object, List<SearchHit>> left = getHitsForIndex(leftIndex, maps);
                Map<Object, List<SearchHit>> right = getHitsForIndex(rightIndex, maps);
                SearchHit[] searchHits = hits.stream()
                    .map(
                        hit -> mergeSource(
                            left.get(hit.joinValue),
                            right.get(hit.joinValue),
                            request.getLeftAlias(),
                            request.getRightAlias()
                        )
                    )
                    .toArray(SearchHit[]::new);
                listener.onResponse(
                    new JoinResponse(new SearchHits(searchHits, new TotalHits(searchHits.length, TotalHits.Relation.EQUAL_TO), 1.0f))
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new RuntimeException(e));
            }
        }, 2);
        assert hitsByIndex.size() == 2;
        getIndexResults(
            hitsByIndex.get(leftIndex),
            targets,
            task,
            request.getLeftIndex(),
            request.getJoinField(),
            leftIndex,
            groupedListener
        );
        getIndexResults(
            hitsByIndex.get(rightIndex),
            targets,
            task,
            request.getRightIndex(),
            request.getJoinField(),
            rightIndex,
            groupedListener
        );

    }

    private static Map<Object, List<SearchHit>> getHitsForIndex(String indexName, Collection<HitsPerIndex> maps) {
        return maps.stream().filter(m -> m.indexName.equals(indexName)).findFirst().map(m -> m.hitsByJoinField).get();
    }

    private SearchHit mergeSource(List<SearchHit> l, List<SearchHit> r, String leftAlias, String rightAlias) {
        // join field isn't unique in my test data... just get the first one out
        SearchHit left = l.get(0);
        SearchHit right = r.get(0);
        leftAlias = leftAlias == null || leftAlias.isEmpty() ? "" : leftAlias.concat(".");
        rightAlias = rightAlias == null || rightAlias.isEmpty() ? "" : rightAlias.concat(".");

        if (l.size() > 1) {
            System.out.println("L SIZE MORE THAN ONE?: " + l);
        }
        if (r.size() > 1) {
            System.out.println("R SIZE MORE THAN ONE?: " + r);
        }

        Map<String, DocumentField> documentFields = new HashMap<>();
        Map<String, DocumentField> metaFields = new HashMap<>();
        left.getFields()
            .forEach(
                (fieldName, docField) -> (MapperService.META_FIELDS_BEFORE_7DOT8.contains(fieldName) ? metaFields : documentFields).put(
                    fieldName,
                    docField
                )
            );
        String combinedId = left.getId() + "|" + right.getId();
        SearchHit searchHit = new SearchHit(left.docId(), combinedId, documentFields, metaFields);
        searchHit.sourceRef(left.getSourceRef());
        searchHit.getSourceAsMap().clear();
        searchHit.getSourceAsMap().putAll(prefixColNames(leftAlias, left));
        searchHit.getSourceAsMap().putAll(prefixColNames(rightAlias, right));
        return searchHit;
    }

    private static Map<String, Object> prefixColNames(String prefix, SearchHit hit) {
        return hit.getSourceAsMap()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> prefix.concat(entry.getKey()),
                    Map.Entry::getValue,
                    (v1, v2) -> v2,  // In case of duplicate keys, keep the last value
                    HashMap::new      // Use HashMap as the map implementation
                )
            );
    }

    private void getIndexResults(
        Map<ShardId, List<Hit>> hitsPerShard,
        Map<String, StreamTargetResponse> targets,
        Task task,
        SearchRequest req,
        String joinField,
        String indexName,
        ActionListener<HitsPerIndex> listener
    ) {
        int count = (int) hitsPerShard.values().stream().mapToInt(List::size).count();
        GroupedActionListener<FetchSearchResult> l = new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Collection<FetchSearchResult> collection) {
                reduceFetchResults(indexName, new ArrayList<>(collection), joinField, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, count);

        for (Map.Entry<ShardId, List<Hit>> entry : hitsPerShard.entrySet()) {
            ShardId shardId = entry.getKey();
            StreamTargetResponse searchPhaseResult = targets.get(shardId.toString());
            SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
            DiscoveryNode node = clusterService.state().nodes().get(searchShardTarget.getNodeId());
            Transport.Connection connection = transportService.getConnection(node);
            ShardFetchSearchRequest fetchRequest = createFetchRequest(
                searchPhaseResult.getQuerySearchResult().getContextId(),
                entry.getValue()
                    .stream()
                    .map(h -> h.leftShardId.getIndexName().equals(shardId.getIndexName()) ? h.leftDocId : h.rightDocId)
                    .collect(Collectors.toList()),
                searchShardTarget.getOriginalIndices(),
                searchPhaseResult.getQuerySearchResult().getShardSearchRequest(),
                searchPhaseResult.getQuerySearchResult().getRescoreDocIds()
            );
            searchTransportService.sendExecuteFetch(
                connection,
                fetchRequest,
                createSearchTask(task, req),
                new SearchActionListener<>(searchShardTarget, shardId.id()) {
                    @Override
                    protected void innerOnResponse(FetchSearchResult result) {
                        l.onResponse(result);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        l.onFailure(e);
                    }
                }
            );
        }
    }

    private void reduceFetchResults(
        String indexName,
        List<FetchSearchResult> fetchResults,
        String joinField,
        ActionListener<HitsPerIndex> listener
    ) {
        try {
            Map<Object, List<SearchHit>> hitsByJoinField = fetchResults.stream()
                .filter(result -> result.hits() != null)
                .flatMap(result -> Arrays.stream(result.hits().getHits()))
                .collect(Collectors.groupingBy(hit -> hit.getSourceAsMap().get(joinField)));
            listener.onResponse(new HitsPerIndex(indexName, hitsByJoinField));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    class HitsPerIndex {
        final String indexName;
        final Map<Object, List<SearchHit>> hitsByJoinField;

        HitsPerIndex(String indexName, Map<Object, List<SearchHit>> hitsByJoinField) {
            this.hitsByJoinField = hitsByJoinField;
            this.indexName = indexName;
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(
        ShardSearchContextId contextId,
        List<Integer> entry,
        OriginalIndices originalIndices,
        ShardSearchRequest shardSearchRequest,
        RescoreDocIds rescoreDocIds
    ) {
        return new ShardFetchSearchRequest(originalIndices, contextId, shardSearchRequest, entry, null, rescoreDocIds, null);
    }

    static class Hit {
        final int leftDocId;
        final ShardId leftShardId;
        final int rightDocId;
        final ShardId rightShardId;
        final Object joinValue;

        Hit(int leftDocId, ShardId leftShardId, int rightDocId, ShardId rightShardId, Object joinValue) {
            this.leftDocId = leftDocId;
            this.leftShardId = leftShardId;
            this.rightDocId = rightDocId;
            this.rightShardId = rightShardId;
            this.joinValue = joinValue;
        }
    }

    public static Map<String, Map<ShardId, List<Hit>>> organizeHitsByIndexAndShard(List<Hit> hits) {
        Map<String, Map<ShardId, List<Hit>>> result = new HashMap<>();

        // Group hits by left index and shards
        Map<String, Map<ShardId, List<Hit>>> leftGroups = hits.stream()
            .collect(
                Collectors.groupingBy(
                    hit -> hit.leftShardId.getIndex().getName(),
                    Collectors.groupingBy(hit -> hit.leftShardId, Collectors.toList())
                )
            );

        // Group hits by right index and shards
        Map<String, Map<ShardId, List<Hit>>> rightGroups = hits.stream()
            .collect(
                Collectors.groupingBy(
                    hit -> hit.rightShardId.getIndex().getName(),
                    Collectors.groupingBy(hit -> hit.rightShardId, Collectors.toList())
                )
            );

        // Combine both maps
        result.putAll(leftGroups);
        result.putAll(rightGroups);

        return result;
    }

    private static Object getValue(FieldVector vector, int index) {
        if (vector == null || vector.isNull(index)) {
            return "null";
        }

        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        } else if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(index));
        } else if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index) != 0;
        }
        // Add more types as needed

        return "Unsupported type: " + vector.getClass().getSimpleName();
    }

    private void searchIndex(Task task, SearchRequest request, GroupedActionListener<SearchResponse> groupedListener) {
        SearchRequest leftRequest = request.searchType(SearchType.STREAM);
        SearchTask leftTask = createSearchTask(task, leftRequest);
        transportSearchAction.doExecute(leftTask, leftRequest, groupedListener);
    }

    private static SearchTask createSearchTask(Task task, SearchRequest request) {
        return request.createTask(task.getId(), task.getType(), task.getAction(), task.getParentTaskId(), Collections.emptyMap());
    }

    List<byte[]> tickets(SearchResponse response) {
        return Objects.requireNonNull(response.getTickets()).stream().map(OSTicket::getBytes).collect(Collectors.toList());
    }
}
