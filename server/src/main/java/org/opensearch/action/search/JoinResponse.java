/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.stream.OSTicket;

import java.io.IOException;

/**
 * Join Response
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class JoinResponse extends ActionResponse {

    private final OSTicket ticket;
    private final SearchHits hits;

    public OSTicket getTicket() {
        return ticket;
    }

    public JoinResponse(OSTicket ticket) {
        this.ticket = ticket;
        this.hits = new SearchHits(new SearchHit[]{}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0f);
    }

    public JoinResponse(SearchHits hits) {
        this.ticket = new OSTicket("", "");
        this.hits = hits;
    }

    public JoinResponse(StreamInput in) throws IOException {
        super(in);
        this.ticket = new OSTicket(in);
        this.hits = new SearchHits(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
       ticket.writeTo(out);
       hits.writeTo(out);
    }

    @Override
    public String toString() {
        return "JoinResponse{" +
            "ticket=" + ticket +
            '}';
    }
}
