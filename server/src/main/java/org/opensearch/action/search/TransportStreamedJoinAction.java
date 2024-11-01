/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.datafusion.DataFrameStreamProducer;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action for initiating a join, gets a single stream back.
 */
public class TransportStreamedJoinAction extends HandledTransportAction<JoinRequest, JoinResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportSearchAction transportSearchAction;
    private final StreamManager streamManager;

    @Inject
    public TransportStreamedJoinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportSearchAction transportSearchAction,
        StreamManager streamManager
    ) {
        super(StreamedJoinAction.NAME, transportService, actionFilters, JoinRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportSearchAction = transportSearchAction;
        this.streamManager = streamManager;
    }

    /**
     * Do the thing
     */
    @Override
    protected void doExecute(Task task, JoinRequest request, ActionListener<JoinResponse> listener) {
        GroupedActionListener<SearchResponse> groupedListener = new GroupedActionListener<>(
            new ActionListener<>() {
                @Override
                public void onResponse(Collection<SearchResponse> collection) {
                    List<SearchResponse> responses = new ArrayList<>(collection);
                    StreamTicket streamTicket = streamManager.registerStream(DataFrameStreamProducer.join(
                        tickets(responses.get(0)),
                        tickets(responses.get(1)),
                        request.getJoinField()
                    ));
                    listener.onResponse(new JoinResponse(new OSTicket(streamTicket.getTicketID(), streamTicket.getNodeID())));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(new RuntimeException(e));
                }
            },
            2
        );
        searchShard(task, request.getLeftIndex(), groupedListener);
        searchShard(task, request.getRightIndex(), groupedListener);
    }

    private void searchShard(Task task, SearchRequest request, GroupedActionListener<SearchResponse> groupedListener) {
        SearchRequest leftRequest = request.searchType(SearchType.STREAM);
        SearchTask leftTask = leftRequest.createTask(
            task.getId(),
            task.getType(),
            task.getAction(),
            task.getParentTaskId(),
            Collections.emptyMap()
        );
        transportSearchAction.doExecute(
            leftTask,
            leftRequest,
            groupedListener
        );
    }

    List<byte[]> tickets(SearchResponse response) {
        return Objects.requireNonNull(response.getTickets()).stream()
            .map(OSTicket::getBytes).collect(Collectors.toList());
    }
}
